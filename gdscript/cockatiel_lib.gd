class_name CockatielClient
extends RefCounted
## Cockatiel chat-engine client for Godot 4.7+ (GDScript).
##
## One file: a hand-written protobuf wire codec (proto3, package
## `cockatiel_protobuf.v1`, message `Container`) plus a WebSocketPeer transport
## implementing the CLIENT_CONTRACT.md wire spec:
##   - single-connection PIN -> JWT auth (no two-phase / port hop)
##   - full 23-field Container oneof decode surface
##   - automatic AuthVerify liveness answers inside the receive loop
##   - reconnect carrying the stored JWT
##   - PIN precedence: COCKATIEL_PIN env -> opts.pin
##   - TLS: when COCKATIEL_TLS_CERT is set (and non-empty), the URL is upgraded
##     ws:// -> wss:// and the engine's self-signed cert is pinned as the trusted
##     chain (strict verification; Godot 4.7 passes TLSOptions to connect_to_url)
##
## Import:  const Cockatiel = preload("res://cockatiel_lib.gd")

# ---------------------------------------------------------------------------
# Protocol constants
# ---------------------------------------------------------------------------

const VERSION := 1
const DEFAULT_URL := "ws://127.0.0.1:9734"
const DEFAULT_TIMEOUT_MS := 10000

# ProcessPosition enum (matches the .proto)
const PROCESS_POSITION := {
	"unspecified": 0,
	"preprocess": 1,
	"inprocess": 2,
	"postprocess": 3,
	"connection": 4,
}

# proto message -> [[field_name, field_number, field_type], ...]
# field_type: int32/int64/uint32/uint64/bool/enum/string/bytes/float(double)/
#             msg:<MsgName>/rep:<inner>/map:<key>:<val>
const _MESSAGES := {
	"AuthNew": [
		["new_auth", 1, "string"],
	],
	"AuthVerify": [
		["cur_auth", 1, "string"],
	],
	"Ban": [
		["commender_uuid7", 1, "string"],
		["commendee_uuid7", 2, "string"],
		["unbanned", 3, "bool"],
		["reason", 4, "string"],
		["raw_message", 5, "string"],
		["appeals", 6, "rep:string"],
	],
	"Flag": [
		["flag_name", 1, "string"],
		["flag_description", 2, "string"],
		["limiting_type", 3, "enum"],
		["min_val", 4, "float"],
		["max_val", 5, "float"],
		["options", 6, "rep:string"],
	],
	"Command": [
		["command_name", 1, "string"],
		["command_flag", 2, "string"],
		["command_description", 3, "string"],
		["command_flags", 4, "rep:msg:Flag"],
	],
	"Commands": [
		["commands", 1, "rep:msg:Command"],
	],
	"Commendation": [
		["commender_uuid7", 1, "string"],
		["commendee_uuid7", 2, "string"],
		["raw_message", 3, "string"],
		["reason", 4, "string"],
	],
	"Reprimand": [
		["commender_uuid7", 1, "string"],
		["commendee_uuid7", 2, "string"],
		["raw_message", 3, "string"],
		["reason", 4, "string"],
	],
	"UserStylingTemplate": [
		["css_properties", 1, "map:string:string"],
	],
	"UserData": [
		["uuid", 1, "string"],
		["username", 2, "string"],
		["is_sponsor", 3, "bool"],
		["is_moderator", 4, "bool"],
		["is_admin", 5, "bool"],
		["is_owner", 6, "bool"],
		["bans", 7, "rep:msg:Ban"],
		["commendations", 8, "rep:msg:Commendation"],
		["styling", 9, "msg:UserStylingTemplate"],
		["platform_ids", 10, "map:string:string"],
	],
	"ConnectionRequest": [
		["pin", 1, "int32"],
		["process_position", 2, "enum"],
		["priority", 3, "uint32"],
		["module_instance_uuid7", 4, "string"],
	],
	"ConnectionRequestReturn": [
		["new_port", 1, "uint32"],
		["module_instance_uuid7", 2, "string"],
	],
	"Err": [
		["log", 1, "string"],
		["blob", 2, "bytes"],
		["trace", 3, "string"],
	],
	"Log": [
		["log", 1, "string"],
		["blob", 2, "bytes"],
	],
	"Shutdown": [
		["reason", 1, "string"],
	],
	"SendToPlatforms": [
		["msg", 1, "string"],
		["level", 2, "enum"],
		["module_uuid7", 3, "string"],
		["pid", 4, "string"],
		["platform", 5, "string"],
		["actor_platform", 6, "string"],
		["actor_handle", 7, "string"],
		["actor_uuid7", 8, "string"],
	],
	"MessageAck": [
		["message_uuid7", 1, "string"],
	],
	"DatabaseQuery": [
		["query_id", 1, "string"],
		["sql", 2, "string"],
		["params", 3, "rep:string"],
	],
	"DatabaseQueryResult": [
		["query_id", 1, "string"],
		["success", 2, "bool"],
		["error", 3, "string"],
		["result_blob", 4, "bytes"],
	],
	"ModuleControl": [
		["action", 1, "enum"],
		["module_name", 2, "string"],
		["autostart", 3, "bool"],
	],
	"ModuleControlResult": [
		["success", 1, "bool"],
		["error", 2, "string"],
		["message", 3, "string"],
	],
	"Prompt": [
		["prompt_id_uuid7", 1, "string"],
		["prompt", 2, "string"],
		["details", 3, "string"],
		["yes_dialog", 4, "string"],
		["no_dialog", 5, "string"],
		["timeout", 6, "uint32"],
		["origin", 7, "string"],
		["origin_uuid7", 8, "string"],
		["instructions", 9, "string"],
		["link", 10, "string"],
		["input_label", 11, "string"],
		["prompt_type", 12, "enum"],
	],
	"PromptResponse": [
		["prompt_id_uuid7", 1, "string"],
		["accepted", 2, "bool"],
		["reason", 3, "string"],
	],
	"AuditFlag": [
		["message_uuid7", 1, "string"],
		["reason", 2, "string"],
		["origin", 3, "string"],
	],
	"ChatMessage": [
		["platform", 1, "string"],
		["raw_data", 2, "bytes"],
		["raw_message", 3, "string"],
		["user_uuid7", 4, "string"],
		["command", 5, "msg:Command"],
		["user_data", 6, "msg:UserData"],
	],
	"MessagePreProcess": [
		["raw_message", 1, "msg:ChatMessage"],
		["message_uuid7", 2, "string"],
		["audio", 3, "bytes"],
		["audio_type", 4, "string"],
	],
	"MessageInProcess": [
		["raw_message", 1, "msg:ChatMessage"],
		["processed_message", 2, "string"],
		["abandon_message", 3, "bool"],
		["message_uuid7", 4, "string"],
		["audio", 5, "bytes"],
		["audio_type", 6, "string"],
	],
	"MessagePostProcess": [
		["raw_message", 1, "msg:ChatMessage"],
		["processed_message", 2, "string"],
		["message_uuid7", 3, "string"],
		["audio", 4, "bytes"],
		["audio_type", 5, "string"],
	],
	"TimelineEvent": [
		["timeline_id_uuid7", 1, "string"],
		["event_type", 2, "enum"],
		["command_flag", 3, "string"],
		["data_blob", 4, "bytes"],
		["error_message", 5, "string"],
		["raw_flags", 6, "string"],
		["message_origin", 7, "string"],
		["stream_origin", 8, "string"],
		["raw_message", 9, "string"],
		["processed_message", 10, "string"],
		["user_uuid7", 11, "string"],
		["version", 12, "uint32"],
	],
}

# Container.payload oneof: [client_name, field_number, proto_message_name]
const _PAYLOAD := [
	["connectionRequest", 7, "ConnectionRequest"],
	["connectionRequestReturn", 8, "ConnectionRequestReturn"],
	["authVerify", 9, "AuthVerify"],
	["authNew", 10, "AuthNew"],
	["commandPayload", 11, "Command"],
	["commandsPayload", 12, "Commands"],
	["messagePreProcess", 13, "MessagePreProcess"],
	["messageInProcess", 14, "MessageInProcess"],
	["messagePostProcess", 15, "MessagePostProcess"],
	["timelineEvent", 16, "TimelineEvent"],
	["userData", 17, "UserData"],
	["shutdown", 18, "Shutdown"],
	["log", 19, "Log"],
	["err", 20, "Err"],
	["sendToPlatforms", 21, "SendToPlatforms"],
	["messageAck", 22, "MessageAck"],
	["databaseQuery", 23, "DatabaseQuery"],
	["databaseQueryResult", 24, "DatabaseQueryResult"],
	["moduleControl", 25, "ModuleControl"],
	["moduleControlResult", 26, "ModuleControlResult"],
	["prompt", 27, "Prompt"],
	["promptResponse", 28, "PromptResponse"],
	["auditFlag", 29, "AuditFlag"],
]

const _PAYLOAD_FIELDS := [
	"connectionRequest", "connectionRequestReturn", "authVerify", "authNew",
	"commandPayload", "commandsPayload", "messagePreProcess",
	"messageInProcess", "messagePostProcess", "timelineEvent", "userData",
	"shutdown", "log", "err", "sendToPlatforms", "messageAck",
	"databaseQuery", "databaseQueryResult", "moduleControl",
	"moduleControlResult", "prompt", "promptResponse", "auditFlag",
]

# ---------------------------------------------------------------------------
# Instance state
# ---------------------------------------------------------------------------

var _ws: WebSocketPeer = null
var _tls: TLSOptions = null              # pinned engine cert; null when COCKATIEL_TLS_CERT unset
var _url := ""
var _module_name := ""
var _module_instance_uuid7 := ""
var _auth_token := ""
var _process_position := 4
var _priority := 100
var _connected := false
var _last_error := ""
var _handlers := {}                # field_name -> Array[Callable]
var _receive_any: Array[Callable] = []
var _field_index := {}             # msg_name -> { field_no -> [name, type] }
var _payload_by_field := {}        # field_no -> [client_name, msg_name]

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

func _init() -> void:
	for msg_name in _MESSAGES:
		var idx := {}
		for f in _MESSAGES[msg_name]:
			idx[f[1]] = [f[0], f[2]]
		_field_index[msg_name] = idx
	for p in _PAYLOAD:
		_payload_by_field[p[1]] = [p[0], p[2]]

## Connects to the engine, performs single-connection PIN auth, stores the JWT
## and keeps the same socket. Blocks up to DEFAULT_TIMEOUT_MS.
func connect_to_engine(opts: Dictionary) -> int:
	_load_local_env()
	_url = str(opts.get("url", DEFAULT_URL))
	_module_name = str(opts.get("module_name", ""))
	_process_position = int(opts.get("process_position", PROCESS_POSITION["connection"]))
	_priority = int(opts.get("priority", 100))
	var pin := _resolve_pin(opts)

	if _module_name == "" or _module_name == "unnamed_module":
		_last_error = "module_name must be set (engine rejects blank/unnamed identities)"
		return ERR_INVALID_PARAMETER

	_ws = WebSocketPeer.new()
	_tls = _tls_options()
	if OS.get_environment("COCKATIEL_TLS_CERT") != "" and _tls == null:
		return ERR_CANT_OPEN  # _last_error set inside _tls_options()
	_url = _upgrade_to_wss(_url)
	var err := _ws.connect_to_url(_url, _tls)
	if err != OK:
		_last_error = "WebSocket connect failed: %d" % err
		return err

	if not _wait_until(Callable(self, "_is_open"), DEFAULT_TIMEOUT_MS):
		_last_error = "Timed out opening WebSocket to %s" % _url
		_ws.close()
		return ERR_TIMEOUT

	var req := {
		"version": VERSION,
		"auth_token": "",
		"module_name": _module_name,
		"module_instance_uuid7": "",
		"connectionRequest": {
			"pin": pin,
			"process_position": _process_position,
			"priority": _priority,
			"module_instance_uuid7": "",
		},
	}
	var werr := _ws.send(encode_container(req))
	if werr != OK:
		_last_error = "send handshake failed: %d" % werr
		return werr

	var deadline := Time.get_ticks_msec() + DEFAULT_TIMEOUT_MS
	while Time.get_ticks_msec() < deadline:
		_ws.poll()
		if _ws.get_ready_state() == WebSocketPeer.STATE_CLOSED:
			_last_error = "Connection closed during handshake"
			return ERR_CONNECTION_ERROR
		while _ws.get_available_packet_count() > 0:
			var container := decode_container(_ws.get_packet())
			var ret = container.get("connectionRequestReturn")
			if ret is Dictionary:
				if ret.get("new_port", 0) != 0:
					_last_error = "Engine requested unsupported port hop"
					_ws.close()
					return ERR_CONNECTION_ERROR
				var token := str(container.get("auth_token", ""))
				if token == "":
					_last_error = "Engine rejected connection (no auth token)"
					_ws.close()
					return ERR_UNAUTHORIZED
				_auth_token = token
				var instance := str(ret.get("module_instance_uuid7", ""))
				if instance == "":
					instance = str(container.get("module_instance_uuid7", ""))
				_module_instance_uuid7 = instance
				_connected = true
				return OK
		OS.delay_msec(2)
	_last_error = "Timed out waiting for ConnectionRequestReturn"
	_ws.close()
	return ERR_TIMEOUT

## Call every frame. Reads frames, decodes Containers, auto-answers AuthVerify
## and dispatches to registered handlers.
func poll() -> void:
	if _ws == null or not _connected:
		return
	_ws.poll()
	var state := _ws.get_ready_state()
	if state == WebSocketPeer.STATE_OPEN:
		while _ws.get_available_packet_count() > 0:
			_handle_packet(_ws.get_packet())
	elif state == WebSocketPeer.STATE_CLOSED:
		_connected = false
		_last_error = "Connection closed by peer"

## Alias of poll().
func process() -> void:
	poll()

## Opens a fresh socket carrying the stored JWT (no PIN needed). The engine
## recognizes a valid auth_token as a reauth. The first frame on any new socket
## MUST still be a ConnectionRequest (the engine gates the first message on that
## payload type), so the reauth container carries a connection_request payload
## alongside the token — the reconnection branch ignores its contents.
func reconnect() -> int:
	if _auth_token == "":
		_last_error = "Cannot reconnect without an auth token"
		return ERR_UNAUTHORIZED
	var ws := WebSocketPeer.new()
	var err := ws.connect_to_url(_url, _tls)
	if err != OK:
		_last_error = "WebSocket connect failed: %d" % err
		return err
	var old := _ws
	_ws = ws
	var deadline := Time.get_ticks_msec() + DEFAULT_TIMEOUT_MS
	while Time.get_ticks_msec() < deadline:
		_ws.poll()
		if _ws.get_ready_state() == WebSocketPeer.STATE_OPEN:
			var reauth := {
				"version": VERSION,
				"auth_token": _auth_token,
				"module_name": _module_name,
				"module_instance_uuid7": _module_instance_uuid7,
				"connectionRequest": {
					"pin": 0,
					"process_position": _process_position,
					"priority": _priority,
					"module_instance_uuid7": _module_instance_uuid7,
				},
			}
			var werr := _ws.send(encode_container(reauth))
			_connected = werr == OK
			if old != null:
				old.close()
			return werr
		OS.delay_msec(2)
	_last_error = "Reconnect timed out"
	_ws = old
	return ERR_TIMEOUT

## Closes the socket (best-effort shutdown payload, then close). Named `close`
## because `Object` already owns `disconnect(signal, callable)`.
func close() -> void:
	if _ws == null:
		return
	if _connected and _ws.get_ready_state() == WebSocketPeer.STATE_OPEN:
		send_payload("shutdown", {"reason": ""})
	_ws.close()
	_connected = false

# ---------------------------------------------------------------------------
# Sending / handlers
# ---------------------------------------------------------------------------

## Wraps a payload dict in a Container and sends it. field_name must be one of
## the 23 client payload names (e.g. "log", "messageAck", "promptResponse").
func send_payload(field_name: String, payload: Dictionary) -> int:
	if _ws == null or not _connected:
		_last_error = "Not connected to engine"
		return ERR_UNCONFIGURED
	if _ws.get_ready_state() != WebSocketPeer.STATE_OPEN:
		_last_error = "Socket not open"
		return ERR_CONNECTION_ERROR
	if not _PAYLOAD_FIELDS.has(field_name):
		_last_error = "Unknown payload field: %s" % field_name
		return ERR_INVALID_PARAMETER
	var container := {
		"version": VERSION,
		"auth_token": _auth_token,
		"module_name": _module_name,
		"module_instance_uuid7": _module_instance_uuid7,
	}
	container[field_name] = payload
	var err := _ws.send(encode_container(container))
	if err != OK:
		_last_error = "send failed: %d" % err
	return err

## Registers a typed handler for a payload field name. Chainable.
func on(field_name: String, cb: Callable) -> CockatielClient:
	if not _handlers.has(field_name):
		_handlers[field_name] = []
	var list: Array = _handlers[field_name]
	list.append(cb)
	return self

## Registers an all-catch listener: called with (container, active_field).
## Chainable.
func receive_any(cb: Callable) -> CockatielClient:
	_receive_any.append(cb)
	return self

# ---------------------------------------------------------------------------
# Accessors
# ---------------------------------------------------------------------------

## True once the single-connection auth handshake completed. Named
## `is_connected_to_engine` because `Object` already owns `is_connected(...)`.
func is_connected_to_engine() -> bool:
	return _connected

func get_auth_token() -> String:
	return _auth_token

func get_module_name() -> String:
	return _module_name

func get_module_instance_uuid7() -> String:
	return _module_instance_uuid7

func get_last_error() -> String:
	return _last_error

func get_socket_state() -> int:
	return _ws.get_ready_state() if _ws != null else WebSocketPeer.STATE_CLOSED

# ---------------------------------------------------------------------------
# Receive loop internals
# ---------------------------------------------------------------------------

func _handle_packet(pkt: PackedByteArray) -> void:
	if pkt.size() == 0:
		return
	var container := decode_container(pkt)
	var active := active_payload(container)

	# Auto-answer the engine's liveness probe (inside the loop, not a user
	# callback) so a quiet module is never severed as "unresponsive".
	if active == "authVerify":
		if _auth_token != "":
			var reply := {
				"version": VERSION,
				"auth_token": _auth_token,
				"module_name": _module_name,
				"module_instance_uuid7": _module_instance_uuid7,
				"authVerify": {"cur_auth": _auth_token},
			}
			_ws.send(encode_container(reply))
		return

	for cb in _receive_any:
		cb.call(container, active)
	if active != "" and _handlers.has(active):
		var list: Array = _handlers[active]
		for cb in list:
			cb.call(container[active])

func _is_open() -> bool:
	return _ws != null and _ws.get_ready_state() == WebSocketPeer.STATE_OPEN

func _wait_until(cb: Callable, timeout_ms: int) -> bool:
	var deadline := Time.get_ticks_msec() + timeout_ms
	while Time.get_ticks_msec() < deadline:
		_ws.poll()
		if cb.call():
			return true
		OS.delay_msec(2)
	return false

## Returns TLSOptions pinning the engine's self-signed cert (from
## COCKATIEL_TLS_CERT) as the trusted chain, with strict hostname verification.
## Returns null when the env var is unset/empty. On a load failure sets
## _last_error and returns null (caller fails the connect).
func _tls_options() -> TLSOptions:
	var cert_path := OS.get_environment("COCKATIEL_TLS_CERT")
	if cert_path == "":
		return null
	var cert := X509Certificate.new()
	var err := cert.load(cert_path)
	if err != OK:
		_last_error = "Failed to load TLS cert from %s: %d" % [cert_path, err]
		return null
	# Godot 4.7: TLSOptions.client(trusted_chain, common_name_override) — strict
	# (accept_invalid_certificates=false). client_unsafe() is the lax variant.
	return TLSOptions.client(cert, "")

## Upgrades ws:// -> wss:// when TLS is in use (the engine only accepts WSS).
func _upgrade_to_wss(url: String) -> String:
	if OS.get_environment("COCKATIEL_TLS_CERT") == "":
		return url
	if url.begins_with("ws://"):
		return "wss://" + url.substr(5)
	return url

func _load_local_env() -> void:
	var path := "res://.env"
	if not FileAccess.file_exists(path):
		return
	var f := FileAccess.open(path, FileAccess.READ)
	if f == null:
		return
	while not f.eof_reached():
		var line := f.get_line().strip_edges()
		if line.begins_with("#") or line == "":
			continue
		var eq := line.find("=")
		if eq < 0:
			continue
		var k := line.substr(0, eq).strip_edges()
		var v := line.substr(eq + 1).strip_edges()
		if k == "":
			continue
		if OS.get_environment(k) == "":
			OS.set_environment(k, v)
	f.close()

## PIN precedence: COCKATIEL_PIN env -> opts.pin.
func _resolve_pin(opts: Dictionary) -> int:
	var env_pin := OS.get_environment("COCKATIEL_PIN")
	if env_pin != "" and env_pin.is_valid_int():
		return env_pin.to_int()
	if opts.has("pin"):
		return int(opts["pin"])
	return 0

# ---------------------------------------------------------------------------
# Public codec entry points
# ---------------------------------------------------------------------------

## Encodes a Container (header fields + at most one payload) to protobuf bytes.
func encode_container(data: Dictionary) -> PackedByteArray:
	var out := PackedByteArray()
	_encode_field(out, 1, "int32", data.get("version", 0))
	_encode_field(out, 3, "string", data.get("auth_token", ""))
	_encode_field(out, 4, "string", data.get("module_name", ""))
	_encode_field(out, 5, "string", data.get("module_instance_uuid7", ""))
	for p in _PAYLOAD:
		var client_name: String = p[0]
		if data.has(client_name) and data[client_name] != null:
			_write_len_delimited(out, p[1], _encode_message(p[2], data[client_name]))
	return out

## Decodes a full Container; payload oneof fields that are not set stay null.
func decode_container(bytes: PackedByteArray) -> Dictionary:
	var result := {
		"version": 0,
		"auth_token": "",
		"module_name": "",
		"module_instance_uuid7": "",
	}
	for p in _PAYLOAD:
		result[p[0]] = null
	var pos := [0]
	while pos[0] < bytes.size():
		var key := _read_varint(bytes, pos)
		var field_no := key >> 3
		var wire := key & 7
		match field_no:
			1:
				if wire == 0:
					result["version"] = _to_i32(_read_varint(bytes, pos))
				else:
					_skip_field(wire, bytes, pos, field_no)
			3:
				if wire == 2:
					result["auth_token"] = _read_string(bytes, pos)
				else:
					_skip_field(wire, bytes, pos, field_no)
			4:
				if wire == 2:
					result["module_name"] = _read_string(bytes, pos)
				else:
					_skip_field(wire, bytes, pos, field_no)
			5:
				if wire == 2:
					result["module_instance_uuid7"] = _read_string(bytes, pos)
				else:
					_skip_field(wire, bytes, pos, field_no)
			_:
				var p = _payload_by_field.get(field_no)
				if p != null and wire == 2:
					result[p[0]] = _decode_message(p[1], _read_bytes(bytes, pos))
				else:
					_skip_field(wire, bytes, pos, field_no)
	return result

## Encodes any known message (e.g. "Log", "ConnectionRequest") from a dict.
func encode_message(msg_name: String, data: Dictionary) -> PackedByteArray:
	return _encode_message(msg_name, data)

## Decodes any known message from protobuf bytes into a dict (all fields filled
## with defaults, present fields overwritten).
func decode_message(msg_name: String, bytes: PackedByteArray) -> Dictionary:
	return _decode_message(msg_name, bytes)

## Returns the client name of the payload currently set in a decoded container
## ("" if none).
func active_payload(container: Dictionary) -> String:
	for p in _PAYLOAD:
		if container.has(p[0]) and container[p[0]] != null:
			return p[0]
	return ""

# ---------------------------------------------------------------------------
# Codec internals
# ---------------------------------------------------------------------------

func _encode_message(msg_name: String, data: Dictionary) -> PackedByteArray:
	var out := PackedByteArray()
	if not _MESSAGES.has(msg_name):
		push_warning("CockatielClient: unknown message %s" % msg_name)
		return out
	for f in _MESSAGES[msg_name]:
		if not data.has(f[0]):
			continue
		_encode_field(out, f[1], f[2], data[f[0]])
	return out

func _decode_message(msg_name: String, bytes: PackedByteArray) -> Dictionary:
	var result := {}
	if not _MESSAGES.has(msg_name):
		return result
	for f in _MESSAGES[msg_name]:
		result[f[0]] = _default_value(f[2])
	var idx: Dictionary = _field_index[msg_name]
	var pos := [0]
	while pos[0] < bytes.size():
		var key := _read_varint(bytes, pos)
		var field_no := key >> 3
		var wire := key & 7
		if idx.has(field_no):
			var f: Array = idx[field_no]
			_decode_field(result, f[0], f[1], wire, bytes, pos)
		else:
			_skip_field(wire, bytes, pos, field_no)
	return result

func _default_value(ftype: String) -> Variant:
	if ftype == "string":
		return ""
	if ftype == "bytes":
		return PackedByteArray()
	if ftype.begins_with("msg:"):
		return {}
	if ftype.begins_with("rep:"):
		return []
	if ftype.begins_with("map:"):
		return {}
	if ftype in ["float", "double"]:
		return 0.0
	if ftype == "bool":
		return false
	return 0

func _encode_field(out: PackedByteArray, field_no: int, ftype: String, value) -> void:
	match ftype:
		"string":
			var s := str(value)
			if s == "":
				return
			_write_len_delimited(out, field_no, s.to_utf8_buffer())
		"bytes":
			var b := _as_bytes(value)
			if b.size() == 0:
				return
			_write_len_delimited(out, field_no, b)
		"int32", "int64", "uint32", "uint64", "enum":
			var v := int(value)
			if v == 0:
				return
			_write_key(out, field_no, 0)
			_write_varint(out, v)
		"bool":
			if not bool(value):
				return
			_write_key(out, field_no, 0)
			_write_varint(out, 1)
		"float":
			var f := float(value)
			if f == 0.0:
				return
			_write_key(out, field_no, 5)
			var b := PackedByteArray()
			b.resize(4)
			b.encode_float(0, f)
			out.append_array(b)
		"double":
			var f := float(value)
			if f == 0.0:
				return
			_write_key(out, field_no, 1)
			var b := PackedByteArray()
			b.resize(8)
			b.encode_double(0, f)
			out.append_array(b)
		_:
			if ftype.begins_with("msg:"):
				var inner = value if value is Dictionary else {}
				if inner.is_empty():
					return
				_write_len_delimited(out, field_no, _encode_message(ftype.substr(4), inner))
			elif ftype.begins_with("rep:"):
				_encode_repeated(out, field_no, ftype.substr(4), value)
			elif ftype.begins_with("map:"):
				_encode_map(out, field_no, value)
			else:
				push_warning("CockatielClient: unknown field type %s" % ftype)

func _encode_repeated(out: PackedByteArray, field_no: int, inner: String, value) -> void:
	if not value is Array or value.is_empty():
		return
	if inner.begins_with("msg:"):
		for item in value:
			if item is Dictionary and not item.is_empty():
				_write_len_delimited(out, field_no, _encode_message(inner.substr(4), item))
	elif inner in ["string", "bytes"]:
		for item in value:
			_write_len_delimited(out, field_no, _as_bytes(item))
	else:
		# packed numeric repeat
		var payload := PackedByteArray()
		for item in value:
			_encode_scalar_raw(payload, inner, item)
		_write_len_delimited(out, field_no, payload)

func _encode_map(out: PackedByteArray, field_no: int, value) -> void:
	if not value is Dictionary or value.is_empty():
		return
	for k in value:
		var entry := PackedByteArray()
		_write_len_delimited(entry, 1, str(k).to_utf8_buffer())
		_write_len_delimited(entry, 2, str(value[k]).to_utf8_buffer())
		_write_len_delimited(out, field_no, entry)

func _encode_scalar_raw(out: PackedByteArray, inner: String, item) -> void:
	match inner:
		"int32", "int64", "uint32", "uint64", "enum":
			_write_varint(out, int(item))
		"bool":
			_write_varint(out, 1 if bool(item) else 0)
		"float":
			var b := PackedByteArray()
			b.resize(4)
			b.encode_float(0, float(item))
			out.append_array(b)
		"double":
			var b := PackedByteArray()
			b.resize(8)
			b.encode_double(0, float(item))
			out.append_array(b)

func _decode_field(result: Dictionary, name: String, ftype: String, wire: int, bytes: PackedByteArray, pos: Array) -> void:
	var consumed := true
	match ftype:
		"string":
			if wire == 2:
				result[name] = _read_string(bytes, pos)
			else:
				consumed = false
		"bytes":
			if wire == 2:
				result[name] = _read_bytes(bytes, pos)
			else:
				consumed = false
		"int32":
			if wire == 0:
				result[name] = _to_i32(_read_varint(bytes, pos))
			else:
				consumed = false
		"int64":
			if wire == 0:
				result[name] = _read_varint(bytes, pos)
			else:
				consumed = false
		"uint32":
			if wire == 0:
				result[name] = _read_varint(bytes, pos) & 0xFFFFFFFF
			else:
				consumed = false
		"uint64":
			if wire == 0:
				result[name] = _read_varint(bytes, pos)
			else:
				consumed = false
		"bool":
			if wire == 0:
				result[name] = _read_varint(bytes, pos) != 0
			else:
				consumed = false
		"enum":
			if wire == 0:
				result[name] = _read_varint(bytes, pos)
			else:
				consumed = false
		"float":
			if wire == 5:
				result[name] = bytes.decode_float(pos[0])
				pos[0] += 4
			else:
				consumed = false
		"double":
			if wire == 1:
				result[name] = bytes.decode_double(pos[0])
				pos[0] += 8
			else:
				consumed = false
		_:
			if ftype.begins_with("msg:"):
				if wire == 2:
					result[name] = _decode_message(ftype.substr(4), _read_bytes(bytes, pos))
				else:
					consumed = false
			elif ftype.begins_with("rep:"):
				consumed = _decode_repeated(result, name, ftype.substr(4), wire, bytes, pos)
			elif ftype.begins_with("map:"):
				if wire == 2:
					var entry := _read_map_entry(_read_bytes(bytes, pos))
					if entry.has("key"):
						result[name][entry["key"]] = entry.get("value", "")
				else:
					consumed = false
			else:
				consumed = false
	if not consumed:
		_skip_field(wire, bytes, pos, 0)

func _decode_repeated(result: Dictionary, name: String, inner: String, wire: int, bytes: PackedByteArray, pos: Array) -> bool:
	if inner.begins_with("msg:"):
		if wire == 2:
			result[name].append(_decode_message(inner.substr(4), _read_bytes(bytes, pos)))
			return true
		return false
	if inner in ["string", "bytes"]:
		if wire == 2:
			if inner == "string":
				result[name].append(_read_string(bytes, pos))
			else:
				result[name].append(_read_bytes(bytes, pos))
			return true
		return false
	# numeric repeat: packed (wire 2) or unpacked (native wire)
	if wire == 2:
		var blob := _read_bytes(bytes, pos)
		var p := [0]
		while p[0] < blob.size():
			result[name].append(_decode_scalar_raw(inner, blob, p))
		return true
	if wire in [0, 1, 5]:
		result[name].append(_decode_scalar_raw(inner, bytes, pos))
		return true
	return false

func _decode_scalar_raw(inner: String, bytes: PackedByteArray, pos: Array) -> Variant:
	match inner:
		"int32":
			return _to_i32(_read_varint(bytes, pos))
		"int64", "uint64":
			return _read_varint(bytes, pos)
		"uint32":
			return _read_varint(bytes, pos) & 0xFFFFFFFF
		"bool":
			return _read_varint(bytes, pos) != 0
		"enum":
			return _read_varint(bytes, pos)
		"float":
			var f := bytes.decode_float(pos[0])
			pos[0] += 4
			return f
		"double":
			var d := bytes.decode_double(pos[0])
			pos[0] += 8
			return d
	return 0

func _read_map_entry(blob: PackedByteArray) -> Dictionary:
	var entry := {}
	var pos := [0]
	while pos[0] < blob.size():
		var key := _read_varint(blob, pos)
		var field_no := key >> 3
		var wire := key & 7
		if field_no == 1 and wire == 2:
			entry["key"] = _read_string(blob, pos)
		elif field_no == 2 and wire == 2:
			entry["value"] = _read_string(blob, pos)
		else:
			_skip_field(wire, blob, pos, field_no)
	return entry

# --- wire primitives -------------------------------------------------------

func _read_varint(bytes: PackedByteArray, pos: Array) -> int:
	var result := 0
	var shift := 0
	var guard := 0
	while pos[0] < bytes.size() and guard < 10:
		var b := bytes[pos[0]]
		pos[0] += 1
		result |= (b & 0x7F) << shift
		if (b & 0x80) == 0:
			break
		shift += 7
		guard += 1
	return result

func _write_varint(out: PackedByteArray, v: int) -> void:
	if v < 0:
		# sign-extended negative int32/int64 always occupies 10 bytes
		for i in range(9):
			out.append(0xFF)
		out.append(0x01)
		return
	var x := v
	while x >= 0x80:
		out.append((x & 0x7F) | 0x80)
		x = x >> 7
	out.append(x)

func _write_key(out: PackedByteArray, field_no: int, wire: int) -> void:
	_write_varint(out, (field_no << 3) | wire)

func _write_len_delimited(out: PackedByteArray, field_no: int, payload: PackedByteArray) -> void:
	_write_key(out, field_no, 2)
	_write_varint(out, payload.size())
	out.append_array(payload)

func _read_string(bytes: PackedByteArray, pos: Array) -> String:
	var len := _read_varint(bytes, pos)
	if pos[0] + len > bytes.size():
		pos[0] = bytes.size()
		return ""
	var s := bytes.slice(pos[0], pos[0] + len)
	pos[0] += len
	return s.get_string_from_utf8()

func _read_bytes(bytes: PackedByteArray, pos: Array) -> PackedByteArray:
	var len := _read_varint(bytes, pos)
	if pos[0] + len > bytes.size():
		pos[0] = bytes.size()
		return PackedByteArray()
	var b := bytes.slice(pos[0], pos[0] + len)
	pos[0] += len
	return b

func _skip_field(wire: int, bytes: PackedByteArray, pos: Array, _field_no: int) -> void:
	match wire:
		0:
			_read_varint(bytes, pos)
		1:
			pos[0] = min(pos[0] + 8, bytes.size())
		2:
			var len := _read_varint(bytes, pos)
			pos[0] = min(pos[0] + len, bytes.size())
		5:
			pos[0] = min(pos[0] + 4, bytes.size())
		3:
			# start group: skip until matching end group
			while pos[0] < bytes.size():
				var k := _read_varint(bytes, pos)
				var w := k & 7
				var n := k >> 3
				if w == 4:
					return
				_skip_field(w, bytes, pos, n)
		_:
			pos[0] = bytes.size()

func _to_i32(v: int) -> int:
	v &= 0xFFFFFFFF
	if v & 0x80000000:
		v -= 0x100000000
	return v

func _as_bytes(value) -> PackedByteArray:
	if value is PackedByteArray:
		return value
	if value is String:
		return value.to_utf8_buffer()
	return PackedByteArray()

# ---------------------------------------------------------------------------
# UUID7 (RFC 9562 style)
# ---------------------------------------------------------------------------

static var _uuid_counter := 0

## Returns a uuid7 string (32 hex chars, no dashes). Time-ordered.
static func uuid7() -> String:
	_uuid_counter = (_uuid_counter + 1) & 0xFFF
	var ms := int(Time.get_unix_time_from_system() * 1000.0)
	var rng := RandomNumberGenerator.new()
	rng.randomize()
	var rand_a := rng.randi_range(0, 0xFFF)
	var variant: String = ["8", "9", "a", "b"][rng.randi_range(0, 3)]
	var rb_hi := rng.randi_range(0, 0xFFFFFFFF)
	var rb_lo := rng.randi_range(0, 0xFFFFFFF)
	var rand_b := (rb_hi << 28) | rb_lo
	var a := ((rand_a & 0xFF0) | (_uuid_counter & 0x00F))
	return "%012x" % ms + "7" + "%03x" % a + variant + "%015x" % rand_b

# ---------------------------------------------------------------------------
# Codec self-test (round trips + known byte sequences). Run from the test
# scene; see test/test_runner.gd.
# ---------------------------------------------------------------------------

static func codec_self_test() -> Dictionary:
	var lib = load("res://cockatiel_lib.gd").new()
	var failures: Array = []

	# 1. Known byte sequence: Log { log: "hi" } -> 0x0A 0x02 0x68 0x69
	var log_enc: PackedByteArray = lib.encode_message("Log", {"log": "hi"})
	var expected := PackedByteArray([0x0A, 0x02, 0x68, 0x69])
	if log_enc != expected:
		failures.append("Log bytes mismatch: %s != %s" % [log_enc.hex_encode(), expected.hex_encode()])

	# 2. Known byte sequence: ConnectionRequest { pin: 150, priority: 100 }
	var cr_enc: PackedByteArray = lib.encode_message("ConnectionRequest", {"pin": 150, "priority": 100})
	# pin field1 varint(150): 0x08 0x96 0x01 ; priority field3: 0x18 0x64
	var cr_exp := PackedByteArray([0x08, 0x96, 0x01, 0x18, 0x64])
	if cr_enc != cr_exp:
		failures.append("ConnectionRequest bytes mismatch: %s != %s" % [cr_enc.hex_encode(), cr_exp.hex_encode()])

	# 3. Container round trip with nested connectionRequest
	var container := {
		"version": 1,
		"auth_token": "",
		"module_name": "gd-check",
		"module_instance_uuid7": "12345678901234567890123456789012",
		"connectionRequest": {"pin": 849820, "process_position": 4, "priority": 100, "module_instance_uuid7": ""},
	}
	var ct_bytes: PackedByteArray = lib.encode_container(container)
	var ct_dec: Dictionary = lib.decode_container(ct_bytes)
	if ct_dec["version"] != 1 or ct_dec["module_name"] != "gd-check":
		failures.append("Container header round trip failed")
	if lib.active_payload(ct_dec) != "connectionRequest":
		failures.append("active_payload mismatch")
	var cr: Dictionary = ct_dec["connectionRequest"]
	if cr.get("pin", 0) != 849820 or cr.get("priority", 0) != 100 or cr.get("process_position", -1) != 4:
		failures.append("connectionRequest round trip failed: %s" % [cr])

	# 4. Rich nested round trip: UserData (bools, floats, repeated msgs, maps)
	var ud := {
		"uuid": "abc",
		"username": "gd-user",
		"is_sponsor": true,
		"is_moderator": false,
		"bans": [
			{"commender_uuid7": "c1", "commendee_uuid7": "c2", "unbanned": true, "appeals": ["a", "b"]},
			{"reason": "spam", "appeals": []},
		],
		"styling": {"css_properties": {"color": "#fff", "rank": "gold"}},
		"platform_ids": {"twitch": "handle1"},
	}
	var ud_bytes: PackedByteArray = lib.encode_message("UserData", ud)
	var ud_dec: Dictionary = lib.decode_message("UserData", ud_bytes)
	if ud_dec["username"] != "gd-user" or ud_dec["is_sponsor"] != true or ud_dec["is_moderator"] != false:
		failures.append("UserData scalars round trip failed")
	if (ud_dec["bans"] as Array).size() != 2:
		failures.append("UserData bans size mismatch")
	var ban0: Dictionary = (ud_dec["bans"] as Array)[0]
	if ban0.get("unbanned", false) != true or (ban0.get("appeals") as Array).size() != 2:
		failures.append("UserData ban round trip failed")
	var st: Dictionary = ud_dec["styling"]
	if (st.get("css_properties") as Dictionary).get("color", "") != "#fff":
		failures.append("UserData styling map failed")
	if (ud_dec["platform_ids"] as Dictionary).get("twitch", "") != "handle1":
		failures.append("UserData platform_ids map failed")

	# 5. Flag floats + repeated options (float fixed32 path)
	var flag := {"flag_name": "pf", "min_val": 1.5, "max_val": 9.75, "options": ["x", "y"], "limiting_type": 3}
	var fl_bytes: PackedByteArray = lib.encode_message("Flag", flag)
	var fl_dec: Dictionary = lib.decode_message("Flag", fl_bytes)
	if abs(fl_dec["min_val"] - 1.5) > 0.0001 or abs(fl_dec["max_val"] - 9.75) > 0.0001:
		failures.append("Flag floats failed: %s" % [fl_dec])
	if (fl_dec["options"] as Array).size() != 2 or fl_dec["limiting_type"] != 3:
		failures.append("Flag options/enum failed")

	# 6. int32 negative + bytes round trip (Err message)
	var err_obj := {"log": "boom", "blob": PackedByteArray([1, 2, 3, 255]), "trace": "t"}
	var err_bytes: PackedByteArray = lib.encode_message("Err", err_obj)
	var err_dec: Dictionary = lib.decode_message("Err", err_bytes)
	if (err_dec["blob"] as PackedByteArray) != PackedByteArray([1, 2, 3, 255]) or err_dec["log"] != "boom":
		failures.append("Err bytes round trip failed")

	# 7. uuid7 shape check
	var u := uuid7()
	if u.length() != 32 or u.substr(12, 1) != "7":
		failures.append("uuid7 shape failed: %s" % u)

	return {"ok": failures.is_empty(), "failures": failures}