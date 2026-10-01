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
    ContainerForEngine: root.lookupType('cockatiel_protobuf.ContainerForEngine'),
    ContainerForModule: root.lookupType('cockatiel_protobuf.ContainerForModule'),
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

## Single-Connection Handshake

The client uses Cockatiel's single-connection auth (v2 wire, `version = 2`):

1. **Initial Connection**: Connects to the engine URL and transmits a
   `ContainerForEngine` carrying a `ConnectionRequest` (PIN) plus the module
   name.

2. **Auth Reply**: The engine answers on the **same** socket with a
   `ContainerForModule` whose payload is a `ConnectionRequestReturn`
   (`new_port == 0`) carrying the JWT in the container's `auth_token`. The
   client keeps the socket, stores the JWT, and uses it in every later frame.
   There is no dedicated-port hop.

3. **V2 wire**: outbound frames are encoded as `ContainerForEngine`
   (`module_name` required), inbound frames are decoded as `ContainerForModule`
   (no `module_name`). Stage messages with a non-empty `message_uuid7` are
   receipt-acked with a `message_ack` immediately, before processing.
