# Cockatiel Client for JavaScript / Node.js
a way for javascript/typescript files to easily communicate with the cockatiel engine, allowing easy communication


## Installation && requirements

This library requires [`ws`](https://www.npmjs.com/package/ws) for WebSocket communication and [`protobufjs`](https://www.npmjs.com/package/protobufjs) for binary serialization.

(tldR:)
```bash
npm install ws protobufjs

```

## Quick Start

```javascript
import protobuf from 'protobufjs';
import { connectToEngine } from './cockatielClient.js';

async function main() {
  // 1. Load your compiled or dynamic protobuf bundle
  const root = await protobuf.load('cockatiel_protobuf.proto');
  const pb = {
    Container: root.lookupType('cockatiel.v1.Container')
  };

  // 2. Connect to the engine
  const cockatiel_connection = await connectToEngine({
    url: 'ws://127.0.0.1:9000',
    pin: 1234,
    moduleName: 'my-js-module',
    processPosition: 'inprocess',
  }, pb);

  console.log(`Connected with UUID: ${conn.moduleInstanceUuid7}`);

  // 3. Listen for incoming messages
  cockatiel_connection.listen.log((logMsg) => {
    console.log(`[Engine Log] Level ${logMsg.level}: ${logMsg.message}`);
  });

  cockatiel_connection.listen.authNew((auth) => {
    console.log('Received new auth token, updating...');
    conn.setAuthToken(auth.token);
  });

  // 4. Send an outbound message
  cockatiel_connection.send.log({
    message: 'Hello from <your_module_name>!',
    blob: '', /*data if any*/
  });

  // Graceful shutdown on exit
  process.on('SIGINT', async () => {
    await cockatiel_connection.disconnect('Shutting down gracefully');
    process.exit(0);
  });
}

main().catch(console.error);

```

to help with error tracking, debugging, etc, you **should** wrap functions in a tracker so they can be archived in the timeline for debugging later


```javascript
import { EngineError } from './cockatielClient.js';

try {
  await conn.send.log({ message: 'test', level: 0 });
} catch (err) {
  if (err instanceof EngineError) {
    console.error('Engine operation failed:', err.message);
  } else {
    throw err; // Unexpected runtime error
  }
}

```

---

## The Two-Phase Handshake Flow

The client transparently handles Cockatiel's two-phase handshake mechanism:

1. **Initial Connection**: Connects to the initial listening address (`url`) and transmits a `ConnectionRequest` payload container.


2. **Dedicated Port Handshake**: The engine responds with a `ConnectionRequestReturn` containing a dedicated port and an assigned/resolved module instance UUID.


3. **Seamless Reconnection**: The client automatically closes the initial bootstrap socket, opens a new dedicated WebSocket connection on the newly assigned port, and mounts all I/O, error handlers, and dispatch streams there.
