# Cockatiel Client Contract

Every language client in this repo (and the standalone SDKs) implements the
same wire behavior. The wire format is [`cockatiel_protobuf.proto`](cockatiel_protobuf.proto)
(proto3, package `cockatiel_protobuf.v1`), pinned to `vulbyte/cockatiel_proto@0128454`.

The engine is the server; modules are clients. All communication is
**binary protobuf over a single WebSocket**. A container is the only top-level
message; the payload is a oneof.

## 1. Connection — single-connection auth

1. Open **one** WebSocket to `ws://<ip>:<port>`.
2. Send a `Container` with `version=1`, empty `auth_token`, the module's
   `module_name`, and `connection_request`:
   - `pin` — the engine PIN (see PIN precedence below)
   - `process_position` — enum: 0 unspecified, 1 preprocess, 2 inprocess,
     3 postprocess, 4 connection
   - `priority` — u32
   - `module_instance_uuid7` — empty on first connect (engine assigns)
3. The engine replies with a `Container` whose payload is
   `connection_request_return`:
   - success: `new_port == 0` **and** `container.auth_token` carries the JWT.
     **Keep using the same socket.** The JWT is used in every later message.
   - `new_port != 0` is the removed two-phase flow — treat as a protocol error.
4. `module_instance_uuid7` in the return is the engine-assigned instance id;
   use it in every subsequent container.

There is **no** two-phase handshake and **no** port hop.

## 2. PIN precedence

The engine PIN is a secret and must never be required in a checked-in config:

1. `COCKATIEL_PIN` environment variable (delivered by the supervisor) — highest.
2. `--pin` CLI argument (manual-run override).
3. config file `pin` field — lowest; a value of 0 means "unset".

Clients must also load a module-local `.env` (`KEY=VALUE`) into the process
environment at startup (real env wins), so `COCKATIEL_PIN` works without a shell
wrapper. Clients must **never** persist the PIN back into a config file.

## 3. Sending a payload

Every outbound message is a `Container`:
- `version: 1`
- `auth_token: <JWT from auth>`
- `module_name: <the approved name>`
- `module_instance_uuid7: <the assigned instance id>`
- exactly one payload field set.

`send(<payloadType>, <message>)` must map the payload type to the oneof field
automatically and refuse unknown types.

## 4. Receiving — full Container surface

The decoder must handle the **entire** `Container` oneof (23 fields) so the
client is a drop-in for any module position:

| proto field | client name |
|---|---|
| `connection_request` | connectionRequest |
| `connection_request_return` | connectionRequestReturn |
| `auth_verify` | authVerify |
| `auth_new` | authNew |
| `command_payload` | commandPayload |
| `commands_payload` | commandsPayload |
| `message_pre_process` | messagePreProcess |
| `message_in_process` | messageInProcess |
| `message_post_process` | messagePostProcess |
| `timeline_event` | timelineEvent |
| `user_data` | userData |
| `shutdown` | shutdown |
| `log` | log |
| `err` | err |
| `send_to_platforms` | sendToPlatforms |
| `message_ack` | messageAck |
| `database_query` | databaseQuery |
| `database_query_result` | databaseQueryResult |
| `module_control` | moduleControl |
| `module_control_result` | moduleControlResult |
| `prompt` | prompt |
| `prompt_response` | promptResponse |
| `audit_flag` | auditFlag |
| `chat_message_rejected` | chatMessageRejected |

A `ReceiveAny()` / "all" listener gets every decoded container; typed listeners
get the active payload. Malformed frames are ignored, never crash the loop.

`chat_message_rejected` is a module→engine audit record: a module that rejected
a message reports the reason + the original raw message + what it became, so
the engine can log it clearly and persist it as a searchable timeline event
(`command = "chat_rejected"`). The message itself still flows as the module
chose — the record is a log, not a pipeline stop.

## 5. Liveness — answer `AuthVerify`

The engine probes during dead air (≥30s): it sends `auth_verify` and opens a
response window. A client must reply **immediately** on the same socket:

```
Container { auth_token: <jwt>, module_name, module_instance_uuid7,
            auth_verify: { cur_auth: <jwt> } }
```

Any other inbound frame also proves liveness. Missing the reply gets the
session severed as "unresponsive". The probe answer must be automatic (inside
the receive loop), not a user callback, and must not be blocked by slow
user handlers (run slow handlers as background tasks).

## 6. Reconnect

Drop the socket, open a fresh one, and send a `Container` carrying the stored
JWT as `auth_token` (plus `module_name` + `module_instance_uuid7`). The engine
recognizes a valid token as a reauth. PIN is not needed on reconnect.

## 7. Prompts

The engine (or another module) can raise a `Prompt`:
- `prompt_id_uuid7` — echo this back in the response
- `prompt` / `details` / `instructions` — text
- `yes_dialog` / `no_dialog` — y/n labels
- `input_label` — free-text label (legacy heuristic)
- `prompt_type` — enum: 0 UNSPECIFIED, 1 BOOLEAN (y/n), 2 STRING (free text),
  3 CREDENTIAL (free text, masked)
- `timeout`, `origin`, `origin_uuid7`

A client that presents UIs answers with `prompt_response` (same
`prompt_id_uuid7`, `reason` = the answer, `approve` for y/n). A display-only
client may ignore prompts. CREDENTIAL responses travel in `reason` as
plaintext over the socket (do not log them).

## 8. Message flow & acks

- Adapters ingest with `message_pre_process` (empty `message_uuid7` for a brand
  new message).
- A module that finishes its stage sends `message_ack` with the message's
  `message_uuid7`.
- `message_post_process` is the final broadcast to displays.

## 9. UUID7

New message/instance ids use UUID7 (time-ordered). Clients need a uuid7
generator; the engine assigns the module's instance uuid on first connect.

## 10. Identity

The supervisor forces each module's identity via `--name`; the engine rejects
blank/`unnamed_module` identities and binds the JWT's `name` claim to the
container's `module_name`. Clients must let the caller set the module name
(CLI `--name`, config, or constructor) and never default to a blank name.
## 11. Command system (engine-side)

Modules subscribe to chat commands by sending a `Commands` payload
(`commands_payload`) with their `Command` list (each: `command_name`,
`command_flag`, `command_description`, `command_flags`). An EMPTY `commands`
list = catch-all (the module receives every message, command or not).
`alert_on_unknown_command` opts into the apology reply.

The engine parses every raw message: if it starts with a registered flag it
extracts `<command> <flags> <args>` — flag names ship WITHOUT the leading `-`,
`-p 2` and `-p:2` both parse, a bare `-d` is boolean `true`, and flag values
are validated against the owner's `FlagLimitType` (`ANY`/`OPTIONS`/`RANGE`).
The parsed `Command` (with values embedded in `command_flags`) is attached to
`ChatMessage.command`.

Routing: a known command goes ONLY to the owning module + all catch-alls.
Other messages keep the normal fanout. What a module should return depends on
its position: pre-process can return anything, in-process must return a message,
post-process can return anything. The engine's built-in `!help` lists all
registered commands back to the chat.

A client that owns commands must (a) send its `Commands` registration after
auth, and (b) handle `MessagePreProcess` frames whose `raw_message.command`
carries its command (using `command.command_flags` for the parsed flag values).
