# Cockatiel Odin client

A native Odin client for the Cockatiel chat engine. One self-contained package —
**no third-party Odin packages** — everything is hand-rolled on Odin's stdlib
(`core:net`, `core:crypto`, `core:encoding`, `core:strings`, `core:time`):

- hand-rolled **protobuf wire codec** (varint, length-delimited, fixed32,
  packed repeats, maps) covering the **entire** `Container` + all 23 payload
  messages in the oneof (decode everything; encode what a module sends)
- hand-rolled **WebSocket transport** (RFC 6455 upgrade handshake + frame
  encode/decode with masking) on top of `core:net` TCP sockets
- optional **WSS (TLS)**: when `COCKATIEL_TLS_CERT` points at the engine's
  self-signed cert, the socket is wrapped in TLS via a direct OpenSSL
  (`libssl`/`libcrypto`) binding — required since the engine only serves `wss://`
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

> TLS note: `core:net` has no TLS support, so the client binds OpenSSL directly
> with Odin's `foreign` mechanism (`system:ssl` / `system:crypto`). On macOS this
> resolves to Homebrew OpenSSL 3 (`/usr/local/opt/openssl@3/lib` or
> `/opt/homebrew/opt/openssl@3/lib`). Only used when `COCKATIEL_TLS_CERT` is set;
> without it the client stays 100% native over plain `ws://`.

## Usage

Copy the `cockatiel_lib/` directory into your project and import it:

```odin
import "core:fmt"
import ck "cockatiel_lib"

main :: proc() {
	c := ck.new_client()
	defer ck.destroy(&c)

	// Set COCKATIEL_TLS_CERT in the environment to use WSS automatically
	// (or pass a wss:// URL — both trust the engine's self-signed cert).
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
COCKATIEL_TLS_CERT=/path/to/cockatiel-cert.pem odin run . ws://127.0.0.1:9736 123456 cockatiel-test-runner
```

The test connects as `cockatiel-test-runner` (auto-approved by the engine),
ingests a `MessagePreProcess` (empty `message_uuid7`, platform `test`), waits
~150 ms, queries the timeline for the ingested row, and prints `CHAIN_OK`,
exiting 0 on success.

Against a TLS-only engine, set `COCKATIEL_TLS_CERT` to the engine's self-signed
cert (a plain `ws://` URL is then upgraded to TLS automatically; a `wss://` URL
also works). Without it, the client talks plain `ws://` as before.