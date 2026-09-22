# Cockatiel Odin client

A native Odin client for the Cockatiel chat engine. One self-contained package —
**no third-party Odin packages, no C FFI** — everything is hand-rolled on Odin's
stdlib (`core:net`, `core:crypto`, `core:encoding`, `core:strings`, `core:time`):

- hand-rolled **protobuf wire codec** (varint, length-delimited, fixed32,
  packed repeats, maps) covering the **entire** `Container` + all 23 payload
  messages in the oneof (decode everything; encode what a module sends)
- hand-rolled **WebSocket transport** (RFC 6455 upgrade handshake + frame
  encode/decode with masking) on top of `core:net` TCP sockets
- **single-connection auth**: `connect()` sends `ConnectionRequest(PIN)`, reads
  the `ConnectionRequestReturn`, keeps the same socket, stores the JWT
  (no two-phase / port hop)
- **PIN precedence**: `COCKATIEL_PIN` env → explicit `pin` argument, plus a
  module-local `.env` loader
- **automatic AuthVerify liveness answers** inside the receive loop (not a user
  callback, not blocked by slow handlers)
- **reconnect**: fresh socket + `ConnectionRequest` carrying the stored JWT
  (PIN not needed)
- **UUIDv7** generator (RFC 9562, 36-char dashed form)

## Usage

Copy the `cockatiel_lib/` directory into your project and import it:

```odin
import "core:fmt"
import ck "cockatiel_lib"

main :: proc() {
	c := ck.new_client()
	defer ck.destroy(&c)

	if !ck.connect(&c, "ws://127.0.0.1:9734", 0 /* or your PIN */, "my-module") {
		// c.last_error explains the failure
		return
	}

	// register a typed handler for an inbound payload
	ck.register_handler(&c, "databaseQueryResult", proc(client: ^ck.Client, container: ^ck.Container) {
		switch r in container.payload {
		case ck.DatabaseQueryResult:
			// r.result_blob ...
		}
	})

	// send any payload (messageAck, log, promptResponse, sendToPlatforms, ...)
	ck.send(&c, ck.MessageAck{ message_uuid7 = "..." })
	ck.send(&c, ck.Log{ log = "hello from odin" })

	ck.receive_loop(&c) // auto-answers AuthVerify, dispatches callbacks
	ck.reconnect(&c)    // fresh socket carrying the stored JWT
	ck.disconnect(&c)
}
```

## Running the live chain test

Requires an engine. Launch one on an isolated port, e.g.:

```
COCKATIEL_PIN=123456 ./cockatiel-engine-rs --port 9736
```

(`--port`/`--pin` are not parsed by the engine binary — set them via an
isolated `config.json` with `"port": 9736` and `COCKATIEL_PIN` in the
environment, and run the engine from that directory.)

Then run the test:

```
cd test
odin run . ws://127.0.0.1:9736 123456 cockatiel-test-runner
```

The test connects as `cockatiel-test-runner` (auto-approved by the engine),
ingests a `MessagePreProcess` (empty `message_uuid7`, platform `test`), waits
~150 ms, queries the timeline for the ingested row, and prints `CHAIN_OK`,
exiting 0 on success.