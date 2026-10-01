import fs from 'fs';
import WebSocket from 'ws';
const PROTOCOL_VERSION = 2;

/**
 * ContainerForEngine payload oneof fields (module -> engine), camelCase names
 * as protobufjs exposes them. Every outbound frame is encoded as a
 * ContainerForEngine with module_name.
 */
const OUTBOUND_FIELDS = [
  'ban',
  'connectionRequest',
  'authVerify',
  'command',
  'commands',
  'log',
  'err',
  'sendToPlatforms',
  'messageAck',
  'databaseQuery',
  'moduleControl',
  'prompt',
  'promptResponse',
  'auditFlag',
  'chatMessageRejected',
  'predictionUpdate',
  'pollUpdate',
  'channelStats',
  'timelineQuery',
  'queryRequest',
  'userDbRequest',
  'messagePreProcess',
  'messageInProcess',
  'messagePostProcess',
];

/**
 * ContainerForModule payload oneof fields (engine -> module), camelCase names
 * as protobufjs exposes them. Every inbound frame is decoded as a
 * ContainerForModule (no module_name). Full surface — see CLIENT_CONTRACT.md.
 */
const INBOUND_FIELDS = [
  'ban',
  'connectionRequestReturn',
  'authVerify',
  'authNew',
  'commands',
  'messagePreProcess',
  'messageInProcess',
  'messagePostProcess',
  'timelineEvent',
  'userData',
  'shutdown',
  'log',
  'err',
  'sendToPlatforms',
  'databaseQueryResult',
  'moduleControlResult',
  'prompt',
  'promptResponse',
  'timelineQueryResult',
  'queryResponse',
  'userDbResponse',
  'predictionUpdate',
  'pollUpdate',
  'channelStats',
];

/** Stage payloads that must be receipt-acked before processing. */
const STAGE_FIELDS = ['messagePreProcess', 'messageInProcess', 'messagePostProcess'];

/**
 * Build the WebSocket for a given engine URL. When `COCKATIEL_TLS_CERT` is set
 * (and non-empty), connect via `wss://` and pin the given PEM certificate so a
 * self-signed engine cert is accepted. Falls back to plain `ws://` when the env
 * var is unset or the cert can't be read.
 *
 * @param {string} url Engine URL (e.g. "ws://127.0.0.1:9734" or already "wss://")
 * @returns {WebSocket}
 */
function createEngineSocket(url) {
  const cert = process.env.COCKATIEL_TLS_CERT;
  if (cert) {
    let ca;
    try {
      ca = cert.includes('-----BEGIN') ? cert : fs.readFileSync(cert, 'utf8');
    } catch (e) {
      console.warn(`[cockatiel] could not read TLS cert '${cert}': ${e.message}; falling back to ws://`);
    }
    if (ca) {
      return new WebSocket(url.replace(/^ws:\/\//, 'wss://'), {
        ca,
        rejectUnauthorized: true,
      });
    }
  }
  return new WebSocket(url);
}

export class EngineError extends Error {
  constructor(message) {
    super(message);
    this.name = 'EngineError';
  }
}

class HandlerRegistry {
  constructor() {
    this.all = [];
    this.handlers = new Map();
    for (const field of INBOUND_FIELDS) {
      this.handlers.set(field, []);
    }
  }

  dispatch(container) {
    const activeField = INBOUND_FIELDS.find(
      (field) => container[field] !== undefined && container[field] !== null
    );

    // All-catch listeners: (container, activeField)
    for (const cb of this.all) {
      cb(container, activeField);
    }

    // Field-specific listeners
    if (activeField && this.handlers.has(activeField)) {
      for (const cb of this.handlers.get(activeField)) {
        cb(container[activeField]);
      }
    }
  }
}

export class EngineConnection {
  constructor(ws, pb, moduleName, moduleInstanceUuid7, processPosition, priority) {
    this.ws = ws;
    this.pb = pb;
    this.moduleName = moduleName;
    this._moduleInstanceUuid7 = moduleInstanceUuid7;
    this._processPosition = processPosition || '';
    this._priority = priority != null ? priority : 100;
    this._authToken = '';
    this._closing = false;
    this.registry = new HandlerRegistry();

    this.listen = {
      all: (cb) => this.registry.all.push(cb),
    };
    this.send = {};

    // Dynamic registration for listen.<payload>() and send.<payload>().
    // Listeners only know inbound (ContainerForModule) payloads; senders only
    // know outbound (ContainerForEngine) payloads.
    for (const field of INBOUND_FIELDS) {
      this.listen[field] = (cb) => this.registry.handlers.get(field).push(cb);
    }
    for (const field of OUTBOUND_FIELDS) {
      this.send[field] = (data) => {
        if (this.ws.readyState !== WebSocket.OPEN) {
          throw new EngineError('Disconnected from engine');
        }
        const containerObj = {
          version: PROTOCOL_VERSION,
          authToken: this._authToken,
          moduleName: this.moduleName,
          moduleInstanceUuid7: this._moduleInstanceUuid7,
          [field]: data,
        };
        const buffer = this.pb.ContainerForEngine.encode(containerObj).finish();
        this.ws.send(buffer);
      };
    }

    this._setupSocket();
  }

  get moduleInstanceUuid7() {
    return this._moduleInstanceUuid7;
  }

  get authToken() {
    return this._authToken;
  }

  setAuthToken(token) {
    this._authToken = token;
  }

  /** ReceiveAny — a callback for every inbound container. */
  onMessage(cb) {
    this.registry.all.push(cb);
  }

  async disconnect(reason = '') {
    this._closing = true;
    // Shutdown is engine -> module only on the V2 wire; a module just closes.
    this.ws.close();
  }

  _setupSocket() {
    this.ws.on('message', (data, isBinary) => {
      if (!isBinary) return;
      let container;
      try {
        container = this.pb.ContainerForModule.decode(new Uint8Array(data));
      } catch (_err) {
        return; // Ignore decode failures on malformed frames
      }

      // Answer the engine's liveness probe: an AuthVerify request is answered
      // with our own auth token so a quiet module is never severed as
      // "unresponsive" during dead air.
      if (container.authVerify) {
        if (!this._authToken) return;
        try {
          const reply = {
            version: PROTOCOL_VERSION,
            authToken: this._authToken,
            moduleName: this.moduleName,
            moduleInstanceUuid7: this._moduleInstanceUuid7,
            authVerify: { curAuth: this._authToken },
          };
          this.ws.send(this.pb.ContainerForEngine.encode(reply).finish());
        } catch (_err) {
          // ignore
        }
        return;
      }

      // Receipt-ack: a stage message with a non-empty message_uuid7 is acked
      // to the engine immediately, BEFORE any user handler runs.
      for (const field of STAGE_FIELDS) {
        const stage = container[field];
        if (stage && stage.messageUuid7) {
          try {
            const ack = {
              version: PROTOCOL_VERSION,
              authToken: this._authToken,
              moduleName: this.moduleName,
              moduleInstanceUuid7: this._moduleInstanceUuid7,
              messageAck: { messageUuid7: stage.messageUuid7 },
            };
            this.ws.send(this.pb.ContainerForEngine.encode(ack).finish());
          } catch (_err) {
            // ignore
          }
          break;
        }
      }

      this.registry.dispatch(container);
    });
  }

  /**
   * Reconnect to the engine using the stored JWT (name-trust auth). The
   * engine gates the first frame on ANY fresh socket to a ConnectionRequest,
   * so the reauth carries one, with the JWT in auth_token.
   */
  async reconnect() {
    if (!this._authToken) {
      throw new EngineError('Cannot reconnect without an auth token');
    }
    const newWs = createEngineSocket(this.ws.url);
    await new Promise((resolve, reject) => {
      newWs.once('open', resolve);
      newWs.once('error', (e) => reject(new EngineError(`WebSocket error: ${e.message}`)));
    });

    const reauth = {
      version: PROTOCOL_VERSION,
      authToken: this._authToken,
      moduleName: this.moduleName,
      moduleInstanceUuid7: this._moduleInstanceUuid7,
      connectionRequest: {
        pin: 0,
        processPosition: this._processPosition || '',
        priority: this._priority || 100,
        moduleInstanceUuid7: this._moduleInstanceUuid7,
      },
    };

    const oldWs = this.ws;
    this.ws = newWs;
    this._setupSocket();
    this.ws.send(this.pb.ContainerForEngine.encode(reauth).finish());
    try {
      oldWs.close();
    } catch (_err) {
      // ignore
    }
  }
}

/**
 * Connects to the engine with single-connection auth: one WebSocket, a
 * ConnectionRequest (PIN), and the engine replies with a ConnectionRequestReturn
 * carrying a JWT in `container.authToken`. All subsequent messages use that
 * token on the SAME socket (no two-phase port hop).
 *
 * @param {Object} opts
 * @param {string} opts.url Base WebSocket URL (e.g. "ws://127.0.0.1:9734")
 * @param {number|string} opts.pin Engine access PIN (also honors COCKATIEL_PIN env)
 * @param {string} opts.moduleName Module identity
 * @param {string} [opts.processPosition] Position ("preprocess" | "inprocess" | "postprocess" | "connection")
 * @param {number} [opts.priority] Connection priority (default: 100)
 * @param {string} [opts.moduleInstanceUuid7] UUID7 string
 * @param {Object} pb Compiled Protobuf definitions module containing
 *   `ContainerForEngine` (outbound) and `ContainerForModule` (inbound)
 */
export async function connectToEngine(opts, pb) {
  const pin = opts.pin != null ? opts.pin : (process.env.COCKATIEL_PIN != null ? parseInt(process.env.COCKATIEL_PIN, 10) : 0);
  const requestedUuid = opts.moduleInstanceUuid7 || '';

  const ws = createEngineSocket(opts.url);
  await new Promise((resolve, reject) => {
    ws.once('open', resolve);
    ws.once('error', (e) => reject(new EngineError(`WebSocket error: ${e.message}`)));
  });

  const handshakeRequest = {
    version: PROTOCOL_VERSION,
    authToken: '',
    moduleName: opts.moduleName,
    moduleInstanceUuid7: requestedUuid,
    connectionRequest: {
      pin,
      processPosition: opts.processPosition || '',
      priority: opts.priority || 100,
      moduleInstanceUuid7: requestedUuid,
    },
  };

  ws.send(pb.ContainerForEngine.encode(handshakeRequest).finish());

  const handshakeResponse = await new Promise((resolve, reject) => {
    ws.once('message', (data) => resolve(data));
    ws.once('close', () => reject(new EngineError('Connection closed before handshake response')));
    ws.once('error', (e) => reject(new EngineError(`WebSocket error: ${e.message}`)));
  });

  let responseContainer;
  try {
    responseContainer = pb.ContainerForModule.decode(new Uint8Array(handshakeResponse));
  } catch (e) {
    ws.close();
    throw new EngineError(`Failed to decode handshake response: ${e.message}`);
  }

  const ret = responseContainer.connectionRequestReturn;
  if (!ret) {
    ws.close();
    throw new EngineError('Engine rejected the connection request');
  }

  // Single-connection success: newPort === 0 + a JWT on the container. The
  // legacy two-phase flow (newPort != 0) is gone — reject if it ever appears.
  if (ret.newPort !== 0) {
    ws.close();
    throw new EngineError('Engine requested an unsupported port hop');
  }
  if (!responseContainer.authToken) {
    ws.close();
    throw new EngineError('Engine rejected the connection request (no auth token)');
  }

  const resolvedUuid = ret.moduleInstanceUuid7 || requestedUuid;
  const conn = new EngineConnection(ws, pb, opts.moduleName, resolvedUuid, opts.processPosition, opts.priority);
  conn.setAuthToken(responseContainer.authToken);
  return conn;
}