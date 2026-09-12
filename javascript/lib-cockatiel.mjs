import WebSocket from 'ws';
const PROTOCOL_VERSION = 1;

/**
 * Payload field mapping corresponding to Container.payload oneof definitions.
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
    // Find active oneof payload field
    const activeField = PAYLOAD_FIELDS.find(
      (field) => container[field] !== undefined && container[field] !== null
    );

    const payloadData = activeField ? container[activeField] : null;

    // Trigger all-catch listeners
    for (const cb of this.all) {
      cb(container, activeField);
    }

    // Trigger field-specific listeners
    if (activeField && this.handlers.has(activeField)) {
      const fieldData = container[activeField];
      for (const cb of this.handlers.get(activeField)) {
        cb(fieldData);
      }
    }
  }
}

export class EngineConnection {
  constructor(ws, pb, moduleName, moduleInstanceUuid7, outboundTx) {
    this.ws = ws;
    this.pb = pb;
    this.moduleName = moduleName;
    this._moduleInstanceUuid7 = moduleInstanceUuid7;
    this._authToken = '';
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

  setAuthToken(token) {
    this._authToken = token;
  }

  async disconnect(reason = '') {
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
      try {
        const container = this.pb.Container.decode(new Uint8Array(data));
        this.registry.dispatch(container);
      } catch (err) {
        // Ignore decode failures on malformed frames
      }
    });
  }
}

/**
 * Rewrites the port on a ws(s):// URL while keeping scheme and path intact.
 */
function withPort(baseUrl, port) {
  try {
    const url = new URL(baseUrl);
    url.port = port.toString();
    return url.toString();
  } catch (err) {
    throw new EngineError(`Invalid URL: ${baseUrl}`);
  }
}

/**
 * Connects to the engine, performs two-phase handshake, and establishes the dedicated connection.
 * 
 * @param {Object} opts
 * @param {string} opts.url Base WebSocket URL (e.g. "ws://127.0.0.1:9000")
 * @param {number} opts.pin Engine access PIN
 * @param {string} opts.moduleName Module identity
 * @param {string} [opts.processPosition] Position ("preprocess" | "inprocess" | "postprocess" | "connection")
 * @param {number} [opts.priority] Connection priority (default: 0)
 * @param {string} [opts.moduleInstanceUuid7] UUID7 string
 * @param {Object} pb Compiled Protobuf definitions module containing `Container`
 */
export async function connectToEngine(opts, pb) {
  const initialWs = new WebSocket(opts.url);

  await new Promise((resolve, reject) => {
    initialWs.once('open', resolve);
    initialWs.once('error', (e) => reject(new EngineError(`WebSocket error: ${e.message}`)));
  });

  const requestedUuid = opts.moduleInstanceUuid7 || '';
  const handshakeRequest = {
    version: PROTOCOL_VERSION,
    authToken: '',
    moduleName: opts.moduleName,
    moduleInstanceUuid7: requestedUuid,
    connectionRequest: {
      pin: opts.pin,
      processPosition: opts.processPosition || '',
      priority: opts.priority || 0,
      moduleInstanceUuid7: requestedUuid,
    },
  };

  const encodedHandshake = pb.Container.encode(handshakeRequest).finish();
  initialWs.send(encodedHandshake);

  const handshakeResponse = await new Promise((resolve, reject) => {
    initialWs.once('message', (data) => resolve(data));
    initialWs.once('close', () => reject(new EngineError('Connection closed before handshake response')));
    initialWs.once('error', (e) => reject(new EngineError(`WebSocket error: ${e.message}`)));
  });

  let responseContainer;
  try {
    responseContainer = pb.Container.decode(new Uint8Array(handshakeResponse));
  } catch (e) {
    initialWs.close();
    throw new EngineError(`Failed to decode handshake response: ${e.message}`);
  }

  const ret = responseContainer.connectionRequestReturn;
  if (!ret || ret.newPort === 0) {
    initialWs.close();
    throw new EngineError('Engine rejected the connection request');
  }

  const resolvedUuid = ret.moduleInstanceUuid7 || requestedUuid;
  const dedicatedUrl = withPort(opts.url, ret.newPort);

  // Close initial socket and open dedicated socket on the new port
  initialWs.close();

  const dedicatedWs = new WebSocket(dedicatedUrl);
  await new Promise((resolve, reject) => {
    dedicatedWs.once('open', resolve);
    dedicatedWs.once('error', (e) => reject(new EngineError(`Dedicated WebSocket error: ${e.message}`)));
  });

  return new EngineConnection(dedicatedWs, pb, opts.moduleName, resolvedUuid);
}
