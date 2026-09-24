# Cockatiel GDScript client

A one-file Godot 4.7+ client for the Cockatiel chat engine. It ships its own
hand-written **protobuf wire codec** (proto3, package
`cockatiel_protobuf.v1`) — Godot has no protobuf library — plus a
`WebSocketPeer` transport implementing the full `CLIENT_CONTRACT.md` wire spec:

- single-connection PIN → JWT auth (no two-phase / port hop)
- decode of the **entire 23-field `Container` payload oneof**
- encode of every payload (connection_request, auth_verify, log, message_ack,
  message_pre_process, prompt_response, send_to_platforms, …)
- automatic `AuthVerify` liveness answers inside the receive loop
- `reconnect()` on a fresh socket carrying the stored JWT
- PIN precedence: `COCKATIEL_PIN` env → `opts.pin`
- TLS: when `COCKATIEL_TLS_CERT` is set (and non-empty), the URL is upgraded
  `ws://` → `wss://` and the engine's self-signed cert is pinned as the trusted
  chain (strict verification). Godot 4.7 passes `TLSOptions` as the second
  argument to `WebSocketPeer.connect_to_url(url, tls)`.

## One-line import

```gdscript
const Cockatiel = preload("res://cockatiel_lib.gd")
```

## Usage

```gdscript
extends Node

const Cockatiel = preload("res://cockatiel_lib.gd")

var client

func _ready() -> void:
    client = Cockatiel.new()
    # PIN is read from COCKATIEL_PIN (env) first, then opts.pin.
    var err := client.connect_to_engine({
        "url": "ws://127.0.0.1:9734",  # auto-upgraded to wss:// when COCKATIEL_TLS_CERT is set
        "module_name": "my-module",          # engine rejects blank/unnamed
        "process_position": 3,               # 1=preprocess 2=inprocess 3=postprocess 4=connection
        "priority": 100,
        # "pin": 123456,                     # only if COCKATIEL_PIN is unset
    })
    if err != OK:
        push_error("connect failed: " + client.get_last_error())
        return

    client.on("prompt", Callable(self, "_on_prompt"))
    client.on("log", Callable(self, "_on_log"))
    client.receive_any(Callable(self, "_on_any"))  # every decoded container

    client.send_payload("log", {"log": "hello from GDScript"})
    client.send_payload("messageAck", {"message_uuid7": "..."})

func _process(_delta: float) -> void:
    client.poll()   # or client.process() — call every frame

func _on_prompt(p: Dictionary) -> void:
    # auto-reply example for a yes/no prompt
    client.send_payload("promptResponse", {
        "prompt_id_uuid7": p["prompt_id_uuid7"],
        "accepted": true,
        "reason": "approved",
    })

func _on_any(container: Dictionary, active_field: String) -> void:
    print("RX ", active_field, ": ", container)

func _exit_tree() -> void:
    client.close()   # named close(); Object already owns disconnect(signal)
```

## Running the test headless

The test scene connects to the engine as `cockatiel-test-runner` (an
auto-approved trusted module), sends a `Log` and a `DatabaseQuery`, and exits 0
when it receives the engine's `DatabaseQueryResult` reply.

1. Start the engine (PIN is delivered via its `.env` → `COCKATIEL_PIN`):

   ```sh
   cd /path/to/cockatiel_engine-rs
   ./target/debug/cockatiel-engine-rs > /tmp/cockatiel_engine.log 2>&1 &
   ```

2. Run the test, exporting the engine's PIN and TLS cert so the client picks
   them up (the engine only accepts `wss://`; without the cert the client stays
   on `ws://` and the engine rejects it):

   ```sh
   cd /path/to/cockatiel_lib/gdscript
   COCKATIEL_PIN=849820 \
   COCKATIEL_TLS_CERT=/path/to/cockatiel_engine-rs/tls/cockatiel-cert.pem \
   godot --headless --path . --quit-after 1500
   ```

   Exit code `0` = PASS, `1` = FAIL. The engine log should show
   `Auto-approving trusted module: cockatiel-test-runner` and
   `[cockatiel-test-runner] gdscript-test hello from GDScript`.

3. Kill the engine when done (`kill %1` or `pkill -f cockatiel-engine-rs`).

The codec self-test (known byte sequences + round trips for varint, fixed32,
nested messages, repeated fields and maps) runs first inside the test scene, so
codec regressions fail fast even before the socket connects.

## Notes & caveats

- `close()` is named `close()` — GDScript's `Object` already owns
  `disconnect(signal, callable)`, so the engine-close helper can't be called
  `disconnect`.
- **Reconnect first frame must be a `ConnectionRequest`**: the engine gates the
  first message on any new socket to that payload type (`Payload::ConnectionRequest`
  in `main.rs`), even for a JWT reauth. `reconnect()` therefore sends a
  `connectionRequest` payload alongside the stored `auth_token`; the
  reconnection branch ignores its contents. (The JS client's bare-log reauth
  hits this gate and is rejected.)
- **Engine reconnect race (upstream)**: the engine removes the whole
  `AuthSession` when the *old* socket's disconnect cleanup runs (`auth_store.remove`
  in `main.rs`), so after a successful `reconnect()` the fresh socket is severed
  on its *next* frame ("Severed: invalid auth"). This affects every language
  client, not just GDScript; keep using a single socket per session in practice.
- The engine's 40 ms post-auth drain discards any frame a module sends
  immediately after the `ConnectionRequestReturn`; wait a beat before
  pipelining payloads (the test waits ~600 ms).