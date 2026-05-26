const { CortiClient } = require("@corti/sdk");
const debugLogger = require("./debugLogger");
const { getEnvironment: getCortiEnvironmentById } = require("./cortiEnvironments");

const CONNECT_TIMEOUT_MS = 30000;
const FLUSH_TIMEOUT_MS = 5000;
const MAX_DEBUG_STRING_LENGTH = 300;
const AUDIO_BUFFER_MAX_BYTES = 5 * 1024 * 1024;

function buildAudioFormatString(opts = {}) {
  return (
    opts.audioFormat ||
    `audio/pcm; rate=${opts.sampleRate || 16000}; channels=${opts.channels || 1}; bits=${opts.bitsPerSample || 16}`
  );
}

function normalizeLanguage(lang) {
  return lang && lang !== "auto" ? lang : "en";
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

function resolveSdkEnvironment(envConfig, region) {
  // Built-in eu/us environments are available as strings; custom regions get an URL bundle.
  if (envConfig?.id === "eu" || envConfig?.id === "us") {
    return envConfig.id;
  }
  const r = region || envConfig?.region;
  if (!r) {
    throw new Error("Corti custom environment requires a region");
  }
  return {
    base: `https://api.${r}.corti.app/v2`,
    wss: `wss://api.${r}.corti.app/audio-bridge/v2`,
    login: `https://auth.${r}.corti.app/realms`,
    agents: `https://api.${r}.corti.app`,
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
    this.lastMessageSummary = null;

    // Client-credentials token cache, used only when PKCE is unavailable
    this.clientCredsCache = { token: null, expiresAt: 0, configKey: null };
  }

  get completedSegments() {
    return this.finalSegments;
  }

  async _fetchToken() {
    // Prefer PKCE — uses stored refresh token, no client secret required
    if (this.cortiOAuth) {
      try {
        const token = await this.cortiOAuth.getValidAccessToken();
        if (token) return { accessToken: token };
      } catch {
        // Fall through to client_credentials below
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
      Date.now() < this.clientCredsCache.expiresAt - 30000
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
    return { accessToken: data.access_token, expiresIn: data.expires_in };
  }

  _buildClient() {
    const envConfig = this.environmentManager.getCortiEnvironment();
    const tenant = this.environmentManager.getCortiTenant() || envConfig.defaultTenant;
    const sdkEnvironment = resolveSdkEnvironment(envConfig, envConfig.region);

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
    const response = await client.interactions.create({
      encounter: {
        identifier: `openwhispr-${Date.now()}`,
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

  async connect(options = {}) {
    if (this.socket) {
      debugLogger.debug("Corti tearing down stale stream before reconnect");
      this.cleanup();
    }

    this.connectionOptions = {
      language: options.language,
      audioFormat: options.audioFormat,
      sampleRate: options.sampleRate,
      channels: options.channels,
      bitsPerSample: options.bitsPerSample,
    };
    this.accumulatedText = "";
    this.finalSegments = [];
    this.audioBytesSent = 0;
    this.audioBuffer = [];
    this.audioBufferBytes = 0;
    this.hasStreamError = false;
    this.isDisconnecting = false;
    this.lastMessageSummary = null;

    this.client = this._buildClient();

    try {
      this.interactionId = await this._createInteraction(this.client);
    } catch (err) {
      debugLogger.error("Corti interaction create failed", { error: err.message });
      throw err;
    }

    const language = normalizeLanguage(options.language);
    const audioFormat = buildAudioFormatString(options);
    const configuration = {
      transcription: {
        primaryLanguage: language,
        isDiarization: true,
        participants: [{ channel: 0, role: "multiple" }],
      },
      mode: { type: "transcription" },
      audioFormat,
    };

    debugLogger.debug("Corti connecting stream", {
      interactionId: this.interactionId,
      configuration: summarizeDebugValue(configuration),
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
      debugLogger.error("Corti stream connect failed", { error: err.message });
      this.cleanup();
      throw err;
    }

    this.socket = socket;
    this.isConnected = true;
    this.isConfigured = true;

    socket.on("message", (msg) => this._onMessage(msg));
    socket.on("error", (err) => {
      debugLogger.error("Corti stream error", { error: err?.message });
      this.hasStreamError = true;
      this.onError?.(err instanceof Error ? err : new Error(String(err?.message || err)));
    });
    socket.on("close", (event) => {
      const wasActive = this.isConnected;
      const wasDisconnecting = this.isDisconnecting;
      const hadStreamError = this.hasStreamError;
      debugLogger.debug("Corti stream closed", {
        code: event?.code,
        reason: event?.reason,
        wasActive,
        wasDisconnecting,
        hadStreamError,
        audioBytesSent: this.audioBytesSent,
        textLength: this.accumulatedText.length,
      });
      if (this.flushResolve) {
        this.flushResolve({ text: this.accumulatedText });
        this.flushResolve = null;
      }
      this.cleanup();
      if (wasActive && !wasDisconnecting && !hadStreamError) {
        this.onError?.(new Error(`Corti connection lost (code: ${event?.code})`));
      }
    });

    this._flushAudioBuffer();
  }

  _onMessage(message) {
    this.lastMessageSummary = summarizeDebugValue(message);

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
        this.onSessionEnd?.({ text: this.accumulatedText });
        break;

      case "error": {
        this.hasStreamError = true;
        const detail = message.error || {};
        const errMessage = detail.message || detail.code || "Corti streaming error";
        debugLogger.error("Corti error message", {
          error: detail,
          payload: this.lastMessageSummary,
        });
        this.onError?.(new Error(errMessage));
        break;
      }

      default:
        debugLogger.debug("Corti message", { type: message?.type });
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
    for (const chunk of queued) {
      try {
        this.socket.sendAudio(chunk);
        this.audioBytesSent += chunk.length;
      } catch (err) {
        debugLogger.error("Corti pre-config flush error", { error: err.message });
        break;
      }
    }
  }

  async disconnect() {
    debugLogger.debug("Corti disconnect", {
      audioBytesSent: this.audioBytesSent,
      textLength: this.accumulatedText.length,
    });

    if (!this.socket || !this.isConnected) {
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
    const result = await Promise.race([
      new Promise((resolve) => {
        this.flushResolve = resolve;
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

    try {
      this.socket.sendEnd({ type: "end" });
    } catch (err) {
      debugLogger.error("Corti end send error", { error: err.message });
    }

    this.cleanup();
    this.accumulatedText = "";
    this.finalSegments = [];

    return { text: finalText };
  }

  cleanup() {
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
