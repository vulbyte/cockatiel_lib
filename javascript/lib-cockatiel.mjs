import WebSocket from 'ws';
const PROTOCOL_VERSION = 1;

/**
 * Payload field mapping corresponding to Container.payload oneof definitions
 * (camelCase names as protobufjs exposes them). Full 23-field Container
 * surface — see CLIENT_CONTRACT.md.
 */
const PAYLOAD_FIELDS = [
  'connectionRequest',
  'connectionRequestReturn',
  'authVerify',
  'authNew',
  'commandPayload',
  'commandsPayload',
  'messagePreProcess',
  'messageInProcess',
  'messagePostProcess',
  'timelineEvent',
  'userData',
  'shutdown',
  'log',
  'err',
  'sendToPlatforms',
  'messageAck',
  'databaseQuery',
  'databaseQueryResult',
  'moduleControl',
  'moduleControlResult',
  'prompt',
  'promptResponse',
  'auditFlag',
];

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
    for (const field of PAYLOAD_FIELDS) {
      this.handlers.set(field, []);
    }
  }

  dispatch(container) {
    const activeField = PAYLOAD_FIELDS.find(
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
  constructor(ws, pb, moduleName, moduleInstanceUuid7) {
    this.ws = ws;
    this.pb = pb;
    this.moduleName = moduleName;
    this._moduleInstanceUuid7 = moduleInstanceUuid7;
    this._authToken = '';
    this._closing = false;
    this.registry = new HandlerRegistry();

    this.listen = {
      all: (cb) => this.registry.all.push(cb),
    };
    this.send = {};

    // Dynamic registration for listen.<payload>() and send.<payload>()
    for (const field of PAYLOAD_FIELDS) {
      this.listen[field] = (cb) => this.registry.handlers.get(field).push(cb);
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
        const buffer = this.pb.Container.encode(containerObj).finish();
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
    if (this.send.shutdown) {
      try {
        this.send.shutdown({ reason });
      } catch (_) {
        // Socket may already be closed
      }
    }
    this.ws.close();
  }

  _setupSocket() {
    this.ws.on('message', (data, isBinary) => {
      if (!isBinary) return;
      let container;
      try {
        container = this.pb.Container.decode(new Uint8Array(data));
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
          this.ws.send(this.pb.Container.encode(reply).finish());
        } catch (_err) {
          // ignore
        }
        return;
      }

      this.registry.dispatch(container);
    });
  }

  /**
   * Reconnect to the engine using the stored JWT (name-trust auth). The engine
   * recognizes a container carrying a valid auth_token as a reauth on a fresh
   * socket — see the Rust client's reconnect().
   */
  async reconnect() {
    if (!this._authToken) {
      throw new EngineError('Cannot reconnect without an auth token');
    }
    const url = this.ws.url;
    const newWs = new WebSocket(url);
    await new Promise((resolve, reject) => {
      newWs.once('open', resolve);
      newWs.once('error', (e) => reject(new EngineError(`WebSocket error: ${e.message}`)));
    });

    const reauth = {
      version: PROTOCOL_VERSION,
      authToken: this._authToken,
      moduleName: this.moduleName,
      moduleInstanceUuid7: this._moduleInstanceUuid7,
      log: { log: `${this.moduleName} reconnected`, blob: null },
    };

    const oldWs = this.ws;
    this.ws = newWs;
    this._setupSocket();
    this.ws.send(this.pb.Container.encode(reauth).finish());
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
 * @param {Object} pb Compiled Protobuf definitions module containing `Container`
 */
export async function connectToEngine(opts, pb) {
  const pin = opts.pin != null ? opts.pin : (process.env.COCKATIEL_PIN != null ? parseInt(process.env.COCKATIEL_PIN, 10) : 0);
  const requestedUuid = opts.moduleInstanceUuid7 || '';

  const ws = new WebSocket(opts.url);
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

  ws.send(pb.Container.encode(handshakeRequest).finish());

  const handshakeResponse = await new Promise((resolve, reject) => {
    ws.once('message', (data) => resolve(data));
    ws.once('close', () => reject(new EngineError('Connection closed before handshake response')));
    ws.once('error', (e) => reject(new EngineError(`WebSocket error: ${e.message}`)));
  });

  let responseContainer;
  try {
    responseContainer = pb.Container.decode(new Uint8Array(handshakeResponse));
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
  const conn = new EngineConnection(ws, pb, opts.moduleName, resolvedUuid);
  conn.setAuthToken(responseContainer.authToken);
  return conn;
}