# Cockatiel Client Contract

Every language client in this repo (and the standalone SDKs) implements the
same wire behavior. The wire format is the single unified
[`cockatiel_protobuf.proto`](cockatiel_protobuf.proto) (proto3, package
`cockatiel_protobuf`), pinned to `vulbyte/cockatiel_proto`.

The engine is the server; modules are clients. All communication is
**binary protobuf over a single WebSocket**. There is no v1 protocol anymore —
the engine accepts only `version = 2`.

## 0. The two containers

Direction is a type-level contract. A client sends a
**`ContainerForEngine`** (module → engine, **requires `module_name`**) and
receives a **`ContainerForModule`** (engine → module, has **no** `module_name`).
Both carry:

- `version` — MUST be `2`
- `auth_token` — the JWT from auth
- `module_instance_uuid7` — the engine-assigned instance id
- exactly one payload in the `oneof`

Because the shared payload tags are identical across both containers, only the
generated type a side encodes/decodes establishes direction. A client MUST
decode inbound frames as `ContainerForModule` and encode outbound as
`ContainerForEngine`; anything else is a protocol error.

## 1. Connection — single-connection auth

1. Open **one** WebSocket to `ws://<ip>:<port>`.
2. Send a `ContainerForEngine` with `version=2`, empty `auth_token`, the
   module's `module_name`, and `connection_request`:
   - `pin` — the engine PIN (see PIN precedence below)
   - `process_position` — enum: 0 unspecified, 1 preprocess, 2 inprocess,
     3 postprocess, 4 connection
   - `priority` — u32
   - `module_instance_uuid7` — empty on first connect (engine assigns)
3. The engine replies with a `ContainerForModule` whose payload is
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

Every outbound message is a `ContainerForEngine`:

- `version: 2`
- `auth_token: <JWT from auth>`
- `module_name: <the approved name>`
- `module_instance_uuid7: <the assigned instance id>`
- exactly one payload field set.

`send(<payloadType>, <message>)` must map the payload type to the oneof field
automatically and refuse unknown types.

## 4. Receiving — the `ContainerForModule` surface

The decoder must handle the full `ContainerForModule` oneof so the client is a
drop-in for any module position:

| proto field | client name | notes |
|---|---|---|
| `connection_request_return` | connectionRequestReturn | auth reply |
| `auth_verify` | authVerify | liveness probe (answer it) |
| `auth_new` | authNew | credential rotation — adopt `new_auth` |
| `commands` | commands | the engine's registered command set |
| `message_pre_process` | messagePreProcess | STAGE 1 |
| `message_in_process` | messageInProcess | STAGE 2 |
| `message_post_process` | messagePostProcess | STAGE 3 |
| `timeline_event` | timelineEvent | timeline/backup replay |
| `user_data` | userData | a user profile update |
| `shutdown` | shutdown | asked to exit |
| `log` | log | public log line |
| `err` | err | upstream error |
| `send_to_platforms` | sendToPlatforms | post this to the platform(s) |
| `database_query_result` | databaseQueryResult | legacy query reply |
| `module_control_result` | moduleControlResult | module control reply |
| `prompt` | prompt | a question for the user |
| `prompt_response` | promptResponse | your answer to a prompt |
| `timeline_query_result` | timelineQueryResult | reply to a TimelineQuery |
| `query_response` | queryResponse | reply to a QueryRequest |
| `user_db_response` | userDbResponse | reply from the user-database service |
| `prediction_update` | predictionUpdate | live prediction bar |
| `poll_update` | pollUpdate | live free-vote poll |
| `channel_stats` | channelStats | pushed viewer/member counts |

A `ReceiveAny()` / "all" listener gets every decoded container; typed listeners
get the active payload. Malformed frames are ignored, never crash the loop.

## 5. Liveness — answer `AuthVerify`

The engine probes during dead air (≥30s): it sends `auth_verify` and opens a
response window. A client must reply **immediately** on the same socket:

```
ContainerForEngine { auth_token: <jwt>, module_name, module_instance_uuid7,
                     auth_verify: { cur_auth: <jwt> } }
```

Any other inbound frame also proves liveness. Missing the reply gets the
session severed as "unresponsive". The probe answer must be automatic (inside
the receive loop), not a user callback, and must not be blocked by slow
user handlers (run slow handlers as background tasks).

## 6. Reconnect

Drop the socket, open a fresh one, and send a `ContainerForEngine` carrying the
stored JWT as `auth_token` (plus `module_name` + `module_instance_uuid7`). The
engine recognizes a valid token as a reauth. PIN is not needed on reconnect.

## 7. Receipt-ack — deliver, then result

The ack semantic is split in two:

1. **Receipt** — the moment a client RECEIVES a stage message
   (`message_pre_process` / `message_in_process` / `message_post_process`) with
   a non-empty `message_uuid7`, it must send back a `message_ack` immediately,
   BEFORE processing:
   ```
   ContainerForEngine { ..., message_ack: { message_uuid7: <the uuid> } }
   ```
   This confirms delivery. If the engine gets no receipt within the window it
   RESENDS the stage message (capped at 3), distinguishing "didn't get it" from
   "got it but hasn't finished".

2. **Result** — when the client is DONE, it sends the stage message back
   (the stage-echo) with the same `message_uuid7` and whatever it produced:
   - pre-process: `message_pre_process` (content preserved or changed)
   - in-process: `message_in_process` (`processed_message` = what it became,
     `abandon_message` = drop it)
   - post-process: `message_post_process`
   Only the stage-echo advances the pipeline. If the client has nothing to
   return, it just drops and goes idle — the receipt already confirmed delivery.

A stage advances when EVERY module it was sent to has returned its result, or
when the ack timeout expires and the engine's sweep notices. A module that is
not connected is not waited for.

## 8. Queries — never raw SQL

Modules never run raw SQL. There is no SQL escape hatch: a query the engine
does not name is denied. Two typed surfaces replace it:

- **`TimelineQuery`** (module → engine) / `timeline_query_result`
  (engine → module): request timeline events by `timeline_id_uuid7` (single
  event), `event_type`, `platform`, `user_uuid7`, `kind` (flags LIKE),
  `raw_prefix` (raw_message LIKE), `pipeline_status`, `since_ms`, with
  `limit`/`offset` pagination. The reply carries `repeated TimelineEvent`.
- **`QueryRequest`** (module → engine) / `query_response` (engine → module):
  a closed-set named operation. Pick a `QueryOp`, fill `QueryParams`
  (typed fields; the `json` escape carries blobs for the ops still opaque), and
  expect one `QueryResponse` echoing your `request_id`. An operation the caller
  may not run is an error response, not a severed connection.

The legacy `DatabaseQuery` / `database_query_result` pair still exists on the
wire for the named virtual queries the existing modules use, but it is
DEPRECATED — do not write new code against it, and its `sql` field must never
hold arbitrary SQL.

## 9. Prompts

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

## 10. UUID7

New message/instance ids use UUID7 (time-ordered). Clients need a uuid7
generator; the engine assigns the module's instance uuid on first connect.

## 11. Identity

The supervisor forces each module's identity via `--name`; the engine rejects
blank/`unnamed_module` identities and binds the JWT's `name` claim to the
container's `module_name`. Clients must let the caller set the module name
(CLI `--name`, config, or constructor) and never default to a blank name.

## 12. Command system (engine-side)

Modules subscribe to chat commands by sending a `Commands` payload with their
`Command` list (each: `command_name`, `command_flag`, `command_description`,
`command_flags`). An EMPTY `commands` list = catch-all (the module receives
every message, command or not). `alert_on_unknown_command` opts into the
apology reply.

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