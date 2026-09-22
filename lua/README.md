# Cockatiel Lua client (LuaJIT, zero deps)

Pure-LuaJIT client for the Cockatiel chat engine. One self-contained module —
no luarocks, no luasocket, no cjson, no protobuf/websocket libraries. The
WebSocket transport is hand-rolled on raw libc sockets via the FFI, and the
proto3 wire codec is hand-rolled in pure Lua (32-bit split 64-bit varints,
length-delimited, fixed32/64, packed repeats) covering the entire `Container`
plus all 23 payload messages. Implements `CLIENT_CONTRACT.md`.

## Import

```lua
local Cockatiel = require("cockatiel_lib")
```

## Usage

```lua
local Cockatiel = require("cockatiel_lib")

local client = Cockatiel.new({
    url         = "ws://127.0.0.1:9734",   -- ws:// only (no TLS via FFI libc)
    module_name = "my-module",             -- required (engine rejects blank/unnamed)
    pin         = 123456,                  -- optional; COCKATIEL_PIN env wins
    priority    = 100,
    process_position = "connection",       -- "preprocess"|"inprocess"|"postprocess"|"connection"
})

client:connect()                          -- PIN -> JWT on ONE socket

client:on("messagePreProcess", function(msg)
    -- typed handler for any of the 23 payload names
end)
client:receive_any(function(container, active)
    -- all-catch listener: (decoded container, active payload name)
end)

client:send("messagePreProcess", {
    message_uuid7 = "",                   -- empty => engine assigns the row uuid
    raw_message   = { platform = "test", raw_message = "hello" },
})
client:send("databaseQuery", {
    query_id = Cockatiel.uuid7(),
    sql      = "SELECT 1",
    params   = {},
})

client:poll()                             -- non-blocking drain + dispatch
-- or, for a blocking loop:
-- client:receive_loop(function(container, active) ... end)

client:disconnect()
```

Payload names (the 23 `Container` oneof fields): `connectionRequest`,
`connectionRequestReturn`, `authVerify`, `authNew`, `commandPayload`,
`commandsPayload`, `messagePreProcess`, `messageInProcess`,
`messagePostProcess`, `timelineEvent`, `userData`, `shutdown`, `log`, `err`,
`sendToPlatforms`, `messageAck`, `databaseQuery`, `databaseQueryResult`,
`moduleControl`, `moduleControlResult`, `prompt`, `promptResponse`,
`auditFlag`.

## Key behavior

- **Single-connection auth** — `connect()` sends `ConnectionRequest(PIN)` on
  the socket, reads the `ConnectionRequestReturn` on the SAME socket, and
  stores the JWT. `new_port != 0` (the removed two-phase flow) is an error.
- **PIN precedence** — `COCKATIEL_PIN` env var → `opts.pin` → `0`. A module
  local `.env` (`KEY=VALUE`) is loaded into the process environment at startup
  (real env wins).
- **AuthVerify auto-answer** — the receive path replies to the engine's
  liveness probe automatically (inside `poll()` / `receive_loop()`), never in
  a user callback.
- **Reconnect** — `reconnect()` opens a fresh socket and re-authenticates with
  the stored JWT (no PIN). The engine gates the first frame on any new socket
  to a `ConnectionRequest`, so the reauth container carries one alongside the
  token.
- **UUID7** — `Cockatiel.uuid7()` returns a time-ordered 32-hex-char id
  (RFC 9562 style, no dashes).

## Run the live chain test

Requires an engine on port 9738 with PIN `123456`, `cockatiel-test-runner`
module auto-approved (it is hard-coded as trusted in the engine).

```sh
luajit chain_test.lua ws://127.0.0.1:9738 123456 1
```

`chain_test.lua` connects as `cockatiel-test-runner`, runs the offline codec
self-test first (`SELFTEST_OK`), ingests a message with `message_pre_process`
(empty `message_uuid7`), waits ~150 ms, sends a `database_query`
(`SELECT pipeline_status FROM timeline_events WHERE platform='test' AND raw_message='...'`),
and prints `CHAIN_OK` + exits 0 when the result blob has a row. The engine log
shows `Auto-approving trusted module: cockatiel-test-runner`.

## Caveats

- **The engine's `DatabaseQuery` SELECT is slow (~30 s).** The engine build
  ships turso 0.1.5, whose local driver is the Limbo engine; a `DatabaseQuery`
  SELECT resolves only after a ~30 s busy-timeout retry. The chain test's
  per-query deadline is set to 45 s to absorb this. This is engine-side —
  the repo's canonical Rust test-runner (`--suite chain`) hits the same
  latency and fails its 3 s deadline.
- **`--port` / `--pin` CLI flags are ignored by the engine binary.** The
  engine reads its port from `config.json` and its PIN from `.env`
  (`COCKATIEL_PIN`). The chain test therefore runs the engine from an isolated
  working directory carrying its own `config.json` (port 9738) and `.env`
  (PIN 123456).
- **Numeric IPv4 hosts only** for `ws://` URLs (`127.0.0.1`, etc.;
  `localhost` is mapped to `127.0.0.1`). No TLS (`wss://`) — the FFI libc
  socket layer has no TLS.
- **Endianness**: protobuf `float`/`double` are little-endian; the FFI
  pack/unpack byte-swaps on big-endian hosts.
- The connection is single-threaded: use `poll()` from one place, or
  `receive_loop()`; there is no background thread.