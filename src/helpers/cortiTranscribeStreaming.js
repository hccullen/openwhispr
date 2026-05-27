const crypto = require("crypto");
const { CortiClient } = require("@corti/sdk");
const debugLogger = require("./debugLogger");

const CONNECT_TIMEOUT_MS = 30000;
const FLUSH_TIMEOUT_MS = 5000;
const MAX_DEBUG_STRING_LENGTH = 300;
const AUDIO_BUFFER_MAX_BYTES = 5 * 1024 * 1024;
const TOKEN_EXPIRY_BUFFER_MS = 30000;
const WSS_CLOSE_TIMEOUT_MS = 60000;

function buildAudioFormatString(opts = {}) {
  return (
    opts.audioFormat ||
    `audio/pcm; rate=${opts.sampleRate || 16000}; channels=${opts.channels || 1}; bits=${opts.bitsPerSample || 16}`
  );
}

function normalizeLanguage(lang) {
  return lang && lang !== "auto" ? lang : "en";
}

function normalizePunctuationMode(mode) {
  return mode === "spoken" || mode === "off" ? mode : "automatic";
}

const FORMATTING_OPTIONS = {
  dates: new Set(["locale:long", "locale:medium", "locale:short", "iso", "as_dictated"]),
  times: new Set(["locale", "h24", "h12", "as_dictated"]),
  numbers: new Set(["numerals_above_nine", "numerals", "as_dictated"]),
  measurements: new Set(["abbreviated", "as_dictated"]),
  numericRanges: new Set(["numerals", "as_dictated"]),
  ordinals: new Set(["numerals_above_nine", "numerals", "as_dictated"]),
};

function sanitizeFormatting(formatting) {
  if (!formatting || typeof formatting !== "object") return null;
  const out = {};
  for (const [key, allowed] of Object.entries(FORMATTING_OPTIONS)) {
    const v = formatting[key];
    if (typeof v === "string" && allowed.has(v)) out[key] = v;
  }
  return Object.keys(out).length > 0 ? out : null;
}

function truncateDebugString(value) {
  if (typeof value !== "string") return value;
  if (value.length <= MAX_DEBUG_STRING_LENGTH) return value;
  return `${value.slice(0, MAX_DEBUG_STRING_LENGTH)}...`;
}

function summarizeDebugValue(value) {
  if (typeof value === "string") return truncateDebugString(value);
  if (Array.isArray(value)) return value.slice(0, 10).map((item) => summarizeDebugValue(item));
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, entryValue]) => [key, summarizeDebugValue(entryValue)])
  );
}

function buildCortiErrorMessage(detail, fallbackSummary) {
  if (typeof detail?.message === "string" && detail.message.trim()) {
    return detail.message.trim();
  }
  if (typeof detail?.code === "string" && detail.code.trim()) {
    return `Corti streaming error (${detail.code.trim()})`;
  }
  if (fallbackSummary && typeof fallbackSummary === "object") {
    const summarizedKeys = Object.keys(fallbackSummary).filter((key) => key !== "type");
    if (summarizedKeys.length > 0) {
      return `Corti streaming error: ${JSON.stringify(fallbackSummary)}`;
    }
  }
  return "Corti streaming error";
}

function resolveSdkEnvironment(envConfig) {
  // Built-in eu/us are accepted as string literals by the SDK; custom regions
  // need the full URL bundle.
  if (envConfig?.id === "eu" || envConfig?.id === "us") {
    return envConfig.id;
  }
  const region = envConfig?.region;
  if (!region) {
    throw new Error("Corti custom environment requires a region");
  }
  return {
    base: `https://api.${region}.corti.app/v2`,
    wss: `wss://api.${region}.corti.app/audio-bridge/v2`,
    login: `https://auth.${region}.corti.app/realms`,
    agents: `https://api.${region}.corti.app`,
  };
}

class CortiTranscribeStreaming {
  constructor(environmentManager, cortiOAuth) {
    this.environmentManager = environmentManager;
    this.cortiOAuth = cortiOAuth;

    // Public callbacks
    this.onPartialTranscript = null;
    this.onFinalTranscript = null;
    this.onError = null;
    this.onSessionEnd = null;

    // Session state
    this.client = null;
    this.socket = null;
    this.interactionId = null;
    this.isConnected = false;
    this.isConfigured = false;
    this.isDisconnecting = false;
    this.hasStreamError = false;
    this.accumulatedText = "";
    this.finalSegments = [];
    this.audioBytesSent = 0;
    this.audioBuffer = [];
    this.audioBufferBytes = 0;
    this.connectionOptions = null;
    this.flushResolve = null;
    this.lastConfigSummary = null;
    this.lastMessageSummary = null;

    // Warm-idle: keep the WSS open briefly after disconnect so a follow-up
    // recording skips the REST interaction + WSS handshake round-trip.
    this.isWarmIdle = false;
    this.idleCloseTimer = null;

    // Client-credentials token cache, used only when PKCE is unavailable.
    this.clientCredsCache = { token: null, expiresAt: 0, configKey: null };
  }

  get completedSegments() {
    return this.finalSegments;
  }

  async _fetchToken() {
    // Prefer PKCE — uses stored refresh token, no client secret required.
    if (this.cortiOAuth) {
      try {
        const token = await this.cortiOAuth.getValidAccessToken();
        if (token) return { accessToken: token };
      } catch (err) {
        debugLogger.warn("Corti PKCE token fetch failed; falling back to client_credentials", {
          error: err?.message,
        });
      }
    }

    const env = this.environmentManager.getCortiEnvironment();
    const clientId = this.environmentManager.getCortiClientId();
    const clientSecret = this.environmentManager.getCortiClientSecret();
    const region = env.region;
    const tenant = this.environmentManager.getCortiTenant() || env.defaultTenant;

    if (!clientId || !clientSecret) {
      throw new Error(
        "Not connected to Corti. Connect via Settings or configure a Client Secret."
      );
    }

    const configKey = `${clientId}:${region}:${tenant}`;
    if (
      this.clientCredsCache.token &&
      this.clientCredsCache.configKey === configKey &&
      Date.now() < this.clientCredsCache.expiresAt - TOKEN_EXPIRY_BUFFER_MS
    ) {
      return { accessToken: this.clientCredsCache.token };
    }

    const params = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: clientId,
      client_secret: clientSecret,
      scope: "openid",
    });
    const res = await fetch(
      `https://auth.${region}.corti.app/realms/${tenant}/protocol/openid-connect/token`,
      {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: params.toString(),
      }
    );
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`Corti token fetch failed (${res.status}): ${body}`);
    }
    const data = await res.json();
    this.clientCredsCache.token = data.access_token;
    this.clientCredsCache.expiresAt = Date.now() + (data.expires_in || 300) * 1000;
    this.clientCredsCache.configKey = configKey;
    debugLogger.debug("Corti client_credentials token fetched", { expiresIn: data.expires_in });
    return { accessToken: data.access_token, expiresIn: data.expires_in };
  }

  _optionsCompatible(options) {
    if (!this.connectionOptions) return false;
    if (normalizeLanguage(this.connectionOptions.language) !== normalizeLanguage(options.language)) {
      return false;
    }
    if (
      normalizePunctuationMode(this.connectionOptions.punctuationMode) !==
      normalizePunctuationMode(options.punctuationMode)
    ) {
      return false;
    }
    if (
      JSON.stringify(sanitizeFormatting(this.connectionOptions.formatting)) !==
      JSON.stringify(sanitizeFormatting(options.formatting))
    ) {
      return false;
    }
    return buildAudioFormatString(this.connectionOptions) === buildAudioFormatString(options);
  }

  _buildClient() {
    const envConfig = this.environmentManager.getCortiEnvironment();
    const tenant = this.environmentManager.getCortiTenant() || envConfig.defaultTenant;
    const sdkEnvironment = resolveSdkEnvironment(envConfig);

    return new CortiClient({
      environment: sdkEnvironment,
      tenantName: tenant,
      auth: {
        refreshAccessToken: async () => this._fetchToken(),
      },
    });
  }

  async _createInteraction(client) {
    const now = new Date().toISOString();
    // endedAt == startedAt at creation marks the encounter as already wrapped
    // up, so we don't need an explicit close call on disconnect — Corti will
    // garbage-collect it server-side.
    const response = await client.interactions.create({
      encounter: {
        identifier: `openwhispr-${crypto.randomUUID()}`,
        status: "planned",
        type: "first_consultation",
        period: { startedAt: now, endedAt: now },
        title: "Dictation",
      },
    });
    if (!response?.interactionId) {
      throw new Error("Corti create interaction returned no interactionId");
    }
    debugLogger.debug("Corti interaction created", { interactionId: response.interactionId });
    return response.interactionId;
  }

  _buildStreamConfiguration(options) {
    const language = normalizeLanguage(options.language);
    const audioFormat = buildAudioFormatString(options);
    const punctuationMode = normalizePunctuationMode(options.punctuationMode);

    // The SDK's typed StreamConfigTranscription only exposes primaryLanguage /
    // isDiarization / participants, but the serializer is configured to
    // passthrough unrecognized keys. Corti's /audio-bridge endpoint honors
    // automaticPunctuation, spokenPunctuation, and formatting alongside the
    // typed fields, so we attach them here.
    const transcription = {
      primaryLanguage: language,
      isDiarization: true,
      participants: [{ channel: 0, role: "multiple" }],
    };

    // automaticPunctuation and spokenPunctuation are mutually exclusive.
    if (punctuationMode === "spoken") {
      transcription.spokenPunctuation = true;
    } else if (punctuationMode === "off") {
      transcription.automaticPunctuation = false;
    } else {
      transcription.automaticPunctuation = true;
    }

    const formatting = sanitizeFormatting(options.formatting);
    if (formatting) transcription.formatting = formatting;

    return {
      transcription,
      mode: { type: "transcription" },
      audioFormat,
    };
  }

  _resetSessionTranscriptState() {
    this.accumulatedText = "";
    this.finalSegments = [];
    this.audioBytesSent = 0;
    this.hasStreamError = false;
    this.lastMessageSummary = null;
  }

  async connect(options = {}) {
    // Warm-idle reuse: a previous disconnect left the WSS open. If the new
    // options match, skip the REST interaction + WSS handshake.
    if (
      this.isWarmIdle &&
      this.isConnected &&
      this.isConfigured &&
      this.socket &&
      this._optionsCompatible(options)
    ) {
      debugLogger.debug("Corti reusing warm WSS connection");
      clearTimeout(this.idleCloseTimer);
      this.idleCloseTimer = null;
      this.isWarmIdle = false;
      this.isDisconnecting = false;
      this._resetSessionTranscriptState();
      this.audioBuffer = [];
      this.audioBufferBytes = 0;
      return;
    }

    if (this.socket) {
      // Either still connected with incompatible options, or warm-idle expired
      // raced; tear down before starting fresh.
      debugLogger.debug("Corti tearing down stale stream before reconnect", {
        isWarmIdle: this.isWarmIdle,
      });
      this.cleanup();
    }

    this.connectionOptions = {
      language: options.language,
      audioFormat: options.audioFormat,
      sampleRate: options.sampleRate,
      channels: options.channels,
      bitsPerSample: options.bitsPerSample,
      punctuationMode: options.punctuationMode,
      formatting: options.formatting,
    };
    this._resetSessionTranscriptState();
    this.audioBuffer = [];
    this.audioBufferBytes = 0;
    this.isDisconnecting = false;

    this.client = this._buildClient();

    try {
      this.interactionId = await this._createInteraction(this.client);
    } catch (err) {
      debugLogger.error("Corti interaction create failed", { error: err.message });
      this.cleanup();
      throw err;
    }

    const configuration = this._buildStreamConfiguration(options);
    this.lastConfigSummary = summarizeDebugValue(configuration);

    debugLogger.debug("Corti connecting stream", {
      interactionId: this.interactionId,
      configuration: this.lastConfigSummary,
    });

    const connectPromise = this.client.stream.connect({
      id: this.interactionId,
      configuration,
      awaitConfiguration: true,
      connectionTimeoutInSeconds: Math.ceil(CONNECT_TIMEOUT_MS / 1000),
    });

    let socket;
    try {
      socket = await connectPromise;
    } catch (err) {
      debugLogger.error("Corti stream connect failed", {
        error: err.message,
        interactionId: this.interactionId,
        lastConfig: this.lastConfigSummary,
      });
      this.cleanup();
      throw err;
    }

    this.socket = socket;
    this.isConnected = true;
    this.isConfigured = true;

    socket.on("message", (msg) => this._onMessage(msg));
    socket.on("error", (err) => {
      debugLogger.error("Corti stream error", {
        error: err?.message,
        isConnected: this.isConnected,
        isConfigured: this.isConfigured,
        audioBytesSent: this.audioBytesSent,
        textLength: this.accumulatedText.length,
        lastMessage: this.lastMessageSummary,
        lastConfig: this.lastConfigSummary,
      });
      this.hasStreamError = true;
      this.onError?.(err instanceof Error ? err : new Error(String(err?.message || err)));
    });
    socket.on("close", (event) => {
      const wasActive = this.isConnected;
      const wasDisconnecting = this.isDisconnecting;
      const hadStreamError = this.hasStreamError;
      const wasWarmIdle = this.isWarmIdle;
      debugLogger.debug("Corti stream closed", {
        code: event?.code,
        reason: event?.reason,
        wasActive,
        wasDisconnecting,
        hadStreamError,
        wasWarmIdle,
        audioBytesSent: this.audioBytesSent,
        textLength: this.accumulatedText.length,
        lastMessage: this.lastMessageSummary,
      });
      if (this.flushResolve) {
        this.flushResolve({ text: this.accumulatedText });
        this.flushResolve = null;
      }
      this.cleanup();
      if (wasActive && !wasDisconnecting && !hadStreamError && !wasWarmIdle) {
        this.onError?.(new Error(`Corti connection lost (code: ${event?.code})`));
      }
    });

    this._flushAudioBuffer();
  }

  _onMessage(message) {
    this.lastMessageSummary = summarizeDebugValue(message);

    debugLogger.debug("Corti message received", {
      type: message?.type,
      keys: message && typeof message === "object" ? Object.keys(message) : [],
      payload: this.lastMessageSummary,
    });

    switch (message?.type) {
      case "transcript": {
        const segments = Array.isArray(message.data) ? message.data : [];
        for (const segment of segments) {
          const text = (segment.transcript || "").trim();
          if (!text) continue;
          if (segment.final) {
            this.finalSegments.push(text);
            this.accumulatedText = this.finalSegments.join(" ");
            debugLogger.debug("Corti final segment", {
              speakerId: segment.speakerId,
              text: text.slice(0, 100),
              totalLength: this.accumulatedText.length,
            });
            this.onFinalTranscript?.(this.accumulatedText, Date.now(), {
              speakerId: segment.speakerId,
              segmentText: text,
            });
          } else {
            this.onPartialTranscript?.(text);
          }
        }
        break;
      }

      case "flushed":
        debugLogger.debug("Corti flushed");
        if (this.flushResolve) {
          this.flushResolve({ text: this.accumulatedText });
          this.flushResolve = null;
        }
        break;

      case "ENDED":
        debugLogger.debug("Corti stream ENDED");
        // Stop accepting audio immediately — the socket close event will
        // follow shortly and finish teardown.
        this.isConnected = false;
        this.onSessionEnd?.({ text: this.accumulatedText });
        break;

      case "error": {
        this.hasStreamError = true;
        const detail = message.error || {};
        const errMessage = buildCortiErrorMessage(detail, this.lastMessageSummary);
        debugLogger.error("Corti error message", {
          error: detail,
          payload: this.lastMessageSummary,
          isConnected: this.isConnected,
          isConfigured: this.isConfigured,
          audioBytesSent: this.audioBytesSent,
          textLength: this.accumulatedText.length,
          lastConfig: this.lastConfigSummary,
        });
        this.onError?.(new Error(errMessage));
        break;
      }

      default:
        debugLogger.debug("Corti unknown message type", { type: message?.type });
    }
  }

  sendAudio(buffer) {
    if (this.isConnected && this.socket) {
      try {
        this.socket.sendAudio(buffer);
        this.audioBytesSent += buffer.length;
        return true;
      } catch (err) {
        debugLogger.error("Corti sendAudio error", { error: err.message });
        return false;
      }
    }

    // Buffer audio that arrives before the stream is configured so the very
    // first frames aren't dropped during interaction creation + WSS handshake.
    if (this.audioBufferBytes + buffer.length > AUDIO_BUFFER_MAX_BYTES) {
      debugLogger.warn("Corti pre-config audio buffer cap exceeded; dropping frame", {
        bufferedBytes: this.audioBufferBytes,
        droppedBytes: buffer.length,
        isConnected: this.isConnected,
      });
      return false;
    }
    this.audioBuffer.push(buffer);
    this.audioBufferBytes += buffer.length;
    return true;
  }

  _flushAudioBuffer() {
    if (!this.audioBuffer.length || !this.socket) return;
    const queued = this.audioBuffer;
    const queuedBytes = this.audioBufferBytes;
    this.audioBuffer = [];
    this.audioBufferBytes = 0;
    debugLogger.debug("Corti flushing pre-config audio buffer", {
      frames: queued.length,
      bytes: queuedBytes,
    });
    let flushed = 0;
    for (const chunk of queued) {
      try {
        this.socket.sendAudio(chunk);
        this.audioBytesSent += chunk.length;
        flushed += chunk.length;
      } catch (err) {
        debugLogger.error("Corti pre-config flush error", {
          error: err.message,
          flushedBytes: flushed,
        });
        break;
      }
    }
    if (flushed < queuedBytes) {
      debugLogger.warn("Corti pre-config flush incomplete", {
        flushedBytes: flushed,
        totalBytes: queuedBytes,
      });
    }
  }

  async disconnect() {
    debugLogger.debug("Corti disconnect", {
      audioBytesSent: this.audioBytesSent,
      textLength: this.accumulatedText.length,
      isWarmIdle: this.isWarmIdle,
    });

    if (!this.socket) return { text: this.accumulatedText };

    if (this.isWarmIdle) {
      // Stop pressed during the keepalive window with no recording in between;
      // return whatever text we have without disturbing the open WSS.
      return { text: this.accumulatedText };
    }

    if (!this.isConnected) {
      const result = { text: this.accumulatedText };
      this.cleanup();
      this.accumulatedText = "";
      this.finalSegments = [];
      return result;
    }

    this.isDisconnecting = true;

    try {
      this.socket.sendFlush({ type: "flush" });
    } catch (err) {
      debugLogger.error("Corti flush send error", { error: err.message });
    }

    let timeoutId;
    let flushedAcked = false;
    const result = await Promise.race([
      new Promise((resolve) => {
        this.flushResolve = (value) => {
          flushedAcked = true;
          resolve(value);
        };
      }),
      new Promise((resolve) => {
        timeoutId = setTimeout(() => {
          debugLogger.debug("Corti flush timeout, using accumulated text");
          resolve({ text: this.accumulatedText });
        }, FLUSH_TIMEOUT_MS);
      }),
    ]);
    clearTimeout(timeoutId);
    this.flushResolve = null;

    const finalText = result?.text || this.accumulatedText;

    // Only send a graceful end after the server acked our flush. If the flush
    // timed out the connection is already degraded — sending end here can race
    // and drop pending finals.
    if (flushedAcked) {
      try {
        this.socket.sendEnd({ type: "end" });
      } catch (err) {
        debugLogger.error("Corti end send error", { error: err.message });
      }
    }

    // Keep the WSS open briefly so a quick follow-up recording can skip the
    // REST interaction + WSS handshake. The idle timer will tear it down if
    // no new recording arrives.
    this.isWarmIdle = true;
    this.accumulatedText = "";
    this.finalSegments = [];
    this.audioBytesSent = 0;
    clearTimeout(this.idleCloseTimer);
    this.idleCloseTimer = setTimeout(() => {
      debugLogger.debug("Corti idle keepalive expired; closing WSS");
      this.idleCloseTimer = null;
      this.cleanup();
    }, WSS_CLOSE_TIMEOUT_MS);

    return { text: finalText };
  }

  cleanup() {
    clearTimeout(this.idleCloseTimer);
    this.idleCloseTimer = null;

    if (this.socket) {
      try {
        this.socket.close();
      } catch {
        // ignore
      }
      this.socket = null;
    }
    this.client = null;
    this.interactionId = null;
    this.isConnected = false;
    this.isConfigured = false;
    this.isDisconnecting = false;
    this.isWarmIdle = false;
    this.flushResolve = null;
    this.hasStreamError = false;
    this.audioBuffer = [];
    this.audioBufferBytes = 0;
  }

  cleanupAll() {
    this.cleanup();
    this.clientCredsCache = { token: null, expiresAt: 0, configKey: null };
    this.finalSegments = [];
    this.accumulatedText = "";
  }

  getStatus() {
    return {
      isConnected: this.isConnected,
      isConfigured: this.isConfigured,
      audioBytesSent: this.audioBytesSent,
      interactionId: this.interactionId,
    };
  }
}

module.exports = CortiTranscribeStreaming;
