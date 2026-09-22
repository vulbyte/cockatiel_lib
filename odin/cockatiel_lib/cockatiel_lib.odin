package cockatiel_lib

/*
	Cockatiel chat-engine client for Odin (native stdlib only — no third-party
	packages, no C FFI).

	Implements CLIENT_CONTRACT.md:
	  - single-connection PIN -> JWT auth on ONE WebSocket (no two-phase / port hop)
	  - hand-rolled protobuf wire codec covering the ENTIRE `Container` + all 23
	    payload messages in the oneof (decode everything; encode what a module sends)
	  - hand-rolled WebSocket (RFC 6455) over core:net TCP sockets
	  - automatic AuthVerify liveness answers inside the receive loop
	  - reconnect carrying the stored JWT
	  - PIN precedence: COCKATIEL_PIN env -> explicit pin param
	  - RFC 9562 UUIDv7 generator

	Import (copy the `cockatiel_lib` directory into your project):
	    import "cockatiel_lib"
*/

import "core:crypto"
import "core:encoding/base64"
import "core:fmt"
import "core:net"
import "core:os"
import "core:strconv"
import "core:strings"
import "core:time"

// ============================================================================
// Protocol constants (see cockatiel_protobuf.proto)
// ============================================================================

VERSION :: 1
DEFAULT_URL :: "ws://127.0.0.1:9734"

DEFAULT_CONNECT_TIMEOUT :: 10 * time.Second
DEFAULT_RECEIVE_TIMEOUT :: 250 * time.Millisecond
// Settle time after the auth handshake / reconnect send: the engine discards
// any frame that arrives inside its post-auth drain window (~40ms), so a
// client must not fire its first payload until that window has passed.
AUTH_SETTLE_DURATION :: 60 * time.Millisecond
MAX_FRAME_SIZE           :: 256 * 1024

// RFC 6455 handshake GUID.
WS_GUID :: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// ProcessPosition enum.
PROCESS_POSITION_UNSPECIFIED :: 0
PROCESS_POSITION_PREPROCESS  :: 1
PROCESS_POSITION_INPROCESS   :: 2
PROCESS_POSITION_POSTPROCESS :: 3
PROCESS_POSITION_CONNECTION  :: 4

// Container.payload oneof field numbers.
PAYLOAD_CONNECTION_REQUEST          :: 7
PAYLOAD_CONNECTION_REQUEST_RETURN   :: 8
PAYLOAD_AUTH_VERIFY                 :: 9
PAYLOAD_AUTH_NEW                    :: 10
PAYLOAD_COMMAND                     :: 11
PAYLOAD_COMMANDS                    :: 12
PAYLOAD_MESSAGE_PRE_PROCESS         :: 13
PAYLOAD_MESSAGE_IN_PROCESS          :: 14
PAYLOAD_MESSAGE_POST_PROCESS        :: 15
PAYLOAD_TIMELINE_EVENT              :: 16
PAYLOAD_USER_DATA                   :: 17
PAYLOAD_SHUTDOWN                    :: 18
PAYLOAD_LOG                         :: 19
PAYLOAD_ERR                         :: 20
PAYLOAD_SEND_TO_PLATFORMS           :: 21
PAYLOAD_MESSAGE_ACK                 :: 22
PAYLOAD_DATABASE_QUERY              :: 23
PAYLOAD_DATABASE_QUERY_RESULT       :: 24
PAYLOAD_MODULE_CONTROL              :: 25
PAYLOAD_MODULE_CONTROL_RESULT       :: 26
PAYLOAD_PROMPT                      :: 27
PAYLOAD_PROMPT_RESPONSE             :: 28
PAYLOAD_AUDIT_FLAG                  :: 29

// WebSocket opcodes.
OP_CONTINUATION :: 0x0
OP_TEXT         :: 0x1
OP_BINARY       :: 0x2
OP_CLOSE        :: 0x8
OP_PING         :: 0x9
OP_PONG         :: 0xA

// ============================================================================
// Message structs — every proto3 message in the .proto.
// proto3 semantics: zero values are the defaults and are not encoded.
// ============================================================================

AuthNew :: struct {
	new_auth: string,
}

AuthVerify :: struct {
	cur_auth: string,
}

Ban :: struct {
	commender_uuid7: string,
	commendee_uuid7: string,
	unbanned:        bool,
	reason:          string,
	raw_message:     string,
	appeals:         [dynamic]string,
}

Flag :: struct {
	flag_name:        string,
	flag_description: string,
	limiting_type:    i32,
	min_val:          f32,
	max_val:          f32,
	options:          [dynamic]string,
}

Command :: struct {
	command_name:        string,
	command_flag:        string,
	command_description: string,
	command_flags:       [dynamic]Flag,
}

Commands :: struct {
	commands: [dynamic]Command,
}

Commendation :: struct {
	commender_uuid7: string,
	commendee_uuid7: string,
	raw_message:     string,
	reason:          string,
}

Reprimand :: struct {
	commender_uuid7: string,
	commendee_uuid7: string,
	raw_message:     string,
	reason:          string,
}

UserStylingTemplate :: struct {
	css_properties: map[string]string,
}

UserData :: struct {
	uuid:           string,
	username:       string,
	is_sponsor:     bool,
	is_moderator:   bool,
	is_admin:       bool,
	is_owner:       bool,
	bans:           [dynamic]Ban,
	commendations:  [dynamic]Commendation,
	styling:        ^UserStylingTemplate,
	platform_ids:   map[string]string,
}

ConnectionRequest :: struct {
	pin:                  i32,
	process_position:     i32,
	priority:             u32,
	module_instance_uuid7: string,
}

ConnectionRequestReturn :: struct {
	new_port:             u32,
	module_instance_uuid7: string,
}

Err :: struct {
	log:   string,
	blob:  []byte,
	trace: string,
}

Log :: struct {
	log:  string,
	blob: []byte,
}

Shutdown :: struct {
	reason: string,
}

SendToPlatforms :: struct {
	msg:            string,
	level:          i32,
	module_uuid7:   string,
	pid:            string,
	platform:       string,
	actor_platform: string,
	actor_handle:   string,
	actor_uuid7:    string,
}

MessageAck :: struct {
	message_uuid7: string,
}

DatabaseQuery :: struct {
	query_id: string,
	sql:      string,
	params:   [dynamic]string,
}

DatabaseQueryResult :: struct {
	query_id:    string,
	success:     bool,
	error:       string,
	result_blob: []byte,
}

ModuleControl :: struct {
	action:      i32,
	module_name: string,
	autostart:   bool,
}

ModuleControlResult :: struct {
	success: bool,
	error:   string,
	message: string,
}

Prompt :: struct {
	prompt_id_uuid7: string,
	prompt:          string,
	details:         string,
	yes_dialog:      string,
	no_dialog:       string,
	timeout:         u32,
	origin:          string,
	origin_uuid7:    string,
	instructions:    string,
	link:            string,
	input_label:     string,
	prompt_type:     i32,
}

PromptResponse :: struct {
	prompt_id_uuid7: string,
	accepted:        bool,
	reason:          string,
}

AuditFlag :: struct {
	message_uuid7: string,
	reason:        string,
	origin:        string,
}

ChatMessage :: struct {
	platform:    string,
	raw_data:    []byte,
	raw_message: string,
	user_uuid7:  string,
	command:     ^Command,
	user_data:   ^UserData,
}

MessagePreProcess :: struct {
	message_uuid7: string,
	raw_message:   ^ChatMessage,
	audio:         []byte,
	audio_type:    string,
}

MessageInProcess :: struct {
	message_uuid7:    string,
	raw_message:      ^ChatMessage,
	processed_message: string,
	abandon_message:  bool,
	audio:            []byte,
	audio_type:       string,
}

MessagePostProcess :: struct {
	message_uuid7:     string,
	raw_message:       ^ChatMessage,
	processed_message: string,
	audio:             []byte,
	audio_type:        string,
}

TimelineEvent :: struct {
	timeline_id_uuid7: string,
	event_type:        i32,
	command_flag:      string,
	data_blob:         []byte,
	error_message:     string,
	raw_flags:         string,
	message_origin:    string,
	stream_origin:     string,
	raw_message:       string,
	processed_message: string,
	user_uuid7:        string,
	version:           u32,
}

// The Container.payload oneof as an Odin union.
Payload :: union {
	ConnectionRequest,
	ConnectionRequestReturn,
	AuthVerify,
	AuthNew,
	Command,
	Commands,
	MessagePreProcess,
	MessageInProcess,
	MessagePostProcess,
	TimelineEvent,
	UserData,
	Shutdown,
	Log,
	Err,
	SendToPlatforms,
	MessageAck,
	DatabaseQuery,
	DatabaseQueryResult,
	ModuleControl,
	ModuleControlResult,
	Prompt,
	PromptResponse,
	AuditFlag,
}

// The root envelope. payload == nil means no oneof member is set.
Container :: struct {
	version:              i32,
	auth_token:           string,
	module_name:          string,
	module_instance_uuid7: string,
	payload:              Payload,
}

// ============================================================================
// Client
// ============================================================================

Callback :: #type proc(client: ^Client, container: ^Container)

Handler_Entry :: struct {
	name: string,
	cb:   Callback,
}

Client :: struct {
	socket:                net.TCP_Socket,
	host:                  string,
	port:                  int,
	path:                  string,
	module_name:           string,
	module_instance_uuid7: string,
	auth_token:            string,
	process_position:      i32,
	priority:              u32,
	connected:             bool,
	stop:                  bool,
	last_error:            string,
	handlers:              [dynamic]Handler_Entry,
	on_any:                [dynamic]Callback,
}

// Creates a client with default module parameters (position = connection).
new_client :: proc() -> (c: Client) {
	c.process_position = PROCESS_POSITION_CONNECTION
	c.priority = 100
	return
}

destroy :: proc(c: ^Client) {
	if c.connected {
		disconnect(c)
	}
	delete(c.handlers)
	delete(c.on_any)
}

// ----------------------------------------------------------------------------
// PIN precedence: COCKATIEL_PIN env -> explicit pin argument.
// ----------------------------------------------------------------------------
resolve_pin :: proc(explicit: i32) -> i32 {
	env := env_get("COCKATIEL_PIN")
	if env != "" {
		if v, ok := strconv.parse_int(env, 10); ok && v > 0 {
			return i32(v)
		}
	}
	return explicit
}

// Reads an environment variable into a stack buffer (no allocation).
env_get :: proc(key: string) -> string {
	buf: [4096]u8
	return os.get_env_buf(buf[:], key)
}

// Loads a module-local `.env` (KEY=VALUE) into the process environment.
// A real (already-set) environment variable always wins.
load_local_env :: proc() {
	data, err := os.read_entire_file_from_path(".env", context.allocator)
	if err != nil {
		return
	}
	defer delete(data)
	lines := strings.split_lines(string(data))
	defer delete(lines)
	for line in lines {
		ln := strings.trim_space(line)
		if len(ln) == 0 || ln[0] == '#' {
			continue
		}
		eq := strings.index_byte(ln, '=')
		if eq < 0 {
			continue
		}
		k := strings.trim_space(ln[:eq])
		v := strings.trim_space(ln[eq + 1:])
		if len(k) == 0 {
			continue
		}
		if env_get(k) == "" {
			os.set_env(k, v)
		}
	}
}

// ----------------------------------------------------------------------------
// Connection — single-connection PIN -> JWT auth on one socket.
// ----------------------------------------------------------------------------
connect :: proc(
	c: ^Client,
	url: string,
	pin: i32,
	module_name: string,
	process_position: i32 = PROCESS_POSITION_CONNECTION,
	priority: u32 = 100,
) -> bool {
	load_local_env()

	c.module_name = module_name
	c.process_position = process_position
	c.priority = priority

	if c.module_name == "" || c.module_name == "unnamed_module" {
		c.last_error = "module_name must be set (engine rejects blank/unnamed identities)"
		return false
	}

	if !parse_ws_url(url, &c.host, &c.port, &c.path) {
		c.last_error = fmt.aprintf("bad WebSocket URL: %s", url)
		return false
	}

	if !ws_connect(c, DEFAULT_CONNECT_TIMEOUT) {
		return false
	}

	effective_pin := resolve_pin(pin)

	container := Container {
		version     = VERSION,
		auth_token  = "",
		module_name = c.module_name,
		payload     = ConnectionRequest {
			pin              = effective_pin,
			process_position = c.process_position,
			priority         = c.priority,
		},
	}
	if !send_container_raw(c, container) {
		return false
	}

	// Read frames until a ConnectionRequestReturn arrives. The SAME socket is
	// kept; the JWT stored in the return is used for every later message.
	deadline := time.time_add(time.now(), DEFAULT_CONNECT_TIMEOUT)
	for {
		remaining := time.diff(time.now(), deadline)
		if remaining <= 0 {
			c.last_error = "timed out waiting for ConnectionRequestReturn"
			ws_close_socket(c)
			return false
		}
		net.set_option(c.socket, .Receive_Timeout, remaining)

		opcode, payload, closed, ok := ws_read_frame(c, timeout_is_error = true)
		if !ok || closed {
			if c.last_error == "" {
				c.last_error = "connection closed during handshake"
			}
			ws_close_socket(c)
			return false
		}
		if opcode == OP_BINARY {
			dec := decode_container(payload)
			#partial switch ret in dec.payload {
			case ConnectionRequestReturn:
				if ret.new_port != 0 {
					c.last_error = "engine requested unsupported port hop"
					delete(payload)
					ws_close_socket(c)
					return false
				}
				if dec.auth_token == "" {
					c.last_error = "engine rejected connection (no auth token)"
					delete(payload)
					ws_close_socket(c)
					return false
				}
				// strings.clone: decoded strings alias the frame buffer, which is
				// freed below — the token/instance must outlive the frame.
				c.auth_token = strings.clone(dec.auth_token)
				c.module_instance_uuid7 = strings.clone(ret.module_instance_uuid7)
				if c.module_instance_uuid7 == "" {
					c.module_instance_uuid7 = strings.clone(dec.module_instance_uuid7)
				}
				delete(payload)
c.connected = true
			net.set_option(c.socket, .Receive_Timeout, DEFAULT_RECEIVE_TIMEOUT)
			// The engine drains (and discards) frames that arrive while it is
			// still finishing the auth handshake (~40ms window after the
			// ConnectionRequestReturn). Settle past that window so the caller
			// can send its first payload immediately.
			time.sleep(AUTH_SETTLE_DURATION)
			return true
			case Err:
				c.last_error = fmt.aprintf("engine returned err: %s", ret.log)
				delete(payload)
				ws_close_socket(c)
				return false
			}
			delete(payload)
		} else {
			delete(payload)
		}
	}
}

// Sends a payload wrapped in a Container on the live socket.
// `payload` must be one of the Payload union members.
send :: proc(c: ^Client, payload: Payload) -> bool {
	if !c.connected {
		c.last_error = "not connected to engine"
		return false
	}
	container := Container {
		version               = VERSION,
		auth_token            = c.auth_token,
		module_name           = c.module_name,
		module_instance_uuid7 = c.module_instance_uuid7,
		payload               = payload,
	}
	return send_container_raw(c, container)
}

send_container_raw :: proc(c: ^Client, container: Container) -> bool {
	enc := encode_container(container)
	defer delete(enc)
	return ws_send_frame(c, OP_BINARY, enc[:])
}

// ----------------------------------------------------------------------------
// Receive loop — decodes every frame, auto-answers AuthVerify liveness probes,
// and dispatches to registered callbacks. Runs until the socket closes or
// `stop` is set (set from a callback via client.stop = true).
// Returns 0 on a clean stop/close, non-zero on a hard error.
// ----------------------------------------------------------------------------
receive_loop :: proc(c: ^Client) -> int {
	for {
		if c.stop {
			return 0
		}
		opcode, payload, closed, ok := ws_read_frame(c)
		if !ok {
			if c.last_error == "" {
				c.last_error = "receive error"
			}
			return -1
		}
		if closed {
			c.connected = false
			return 0
		}
		if opcode != OP_BINARY || len(payload) == 0 {
			delete(payload)
			continue
		}
		container := decode_container(payload)

		// Automatic liveness answer — inside the loop, not a user callback, so a
		// quiet module is never severed as "unresponsive".
		if _, is_verify := container.payload.(AuthVerify); is_verify {
			if c.auth_token != "" {
				send(c, AuthVerify { cur_auth = c.auth_token })
			}
			delete(payload)
			continue
		}

		active := active_payload(container)
		for cb in c.on_any {
			cb(c, &container)
		}
		if active != "" {
			for h in c.handlers {
				if h.name == active {
					h.cb(c, &container)
				}
			}
		}
		delete(payload)
		if c.stop {
			return 0
		}
	}
}

// Registers a typed handler for a payload field name (e.g. "databaseQueryResult").
register_handler :: proc(c: ^Client, field_name: string, cb: Callback) {
	append(&c.handlers, Handler_Entry { name = field_name, cb = cb })
}

// Registers an all-catch listener: called for every container (container, active field).
register_receive_any :: proc(c: ^Client, cb: Callback) {
	append(&c.on_any, cb)
}

// ----------------------------------------------------------------------------
// Reconnect — fresh socket + ConnectionRequest carrying the stored JWT.
// PIN is not needed; the engine recognizes a valid auth_token as a reauth.
// ----------------------------------------------------------------------------
reconnect :: proc(c: ^Client) -> bool {
	if c.auth_token == "" {
		c.last_error = "cannot reconnect without an auth token"
		return false
	}
	if c.socket != {} {
		net.close(c.socket)
	}
	if !ws_connect(c, DEFAULT_CONNECT_TIMEOUT) {
		return false
	}
	container := Container {
		version               = VERSION,
		auth_token            = c.auth_token,
		module_name           = c.module_name,
		module_instance_uuid7 = c.module_instance_uuid7,
		payload               = ConnectionRequest {
			pin                = 0,
			process_position   = c.process_position,
			priority           = c.priority,
			module_instance_uuid7 = c.module_instance_uuid7,
		},
	}
	if !send_container_raw(c, container) {
		ws_close_socket(c)
		return false
	}
	c.connected = true
	// Same post-auth settle as connect(): the engine drains pre-authorization
	// frames after the reauth too, so wait out that window before returning.
	time.sleep(AUTH_SETTLE_DURATION)
	return true
}

// Closes the socket (best-effort shutdown payload + WS close frame).
disconnect :: proc(c: ^Client) {
	if c.connected {
		send(c, Shutdown { reason = "" })
	}
	ws_close_socket(c)
	c.connected = false
}

// Returns the client name of the payload currently set in a container ("" if none).
active_payload :: proc(c: Container) -> string {
	return payload_name(c.payload)
}

// ----------------------------------------------------------------------------
// Codec entry points
// ----------------------------------------------------------------------------

// Encodes a Container to protobuf bytes. Returns a dynamic array the caller
// must delete().
encode_container :: proc(c: Container) -> [dynamic]u8 {
	w: [dynamic]u8
	put_i32_field(&w, 1, c.version)
	put_string_field(&w, 3, c.auth_token)
	put_string_field(&w, 4, c.module_name)
	put_string_field(&w, 5, c.module_instance_uuid7)
	if c.payload != nil {
		inner := encode_payload(c.payload)
		defer delete(inner)
		put_message_field(&w, payload_field_number(c.payload), inner[:])
	}
	return w
}

// Decodes a full Container from protobuf bytes.
decode_container :: proc(b: []byte) -> (c: Container) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if v, ok := read_varint_field(&r, wire); ok {
				c.version = i32(v)
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				c.auth_token = s
			}
		case 4:
			if s, ok := read_string_field(&r, wire); ok {
				c.module_name = s
			}
		case 5:
			if s, ok := read_string_field(&r, wire); ok {
				c.module_instance_uuid7 = s
			}
		case PAYLOAD_CONNECTION_REQUEST:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = ConnectionRequest(decode_connection_request(pb))
			}
		case PAYLOAD_CONNECTION_REQUEST_RETURN:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = ConnectionRequestReturn(decode_connection_request_return(pb))
			}
		case PAYLOAD_AUTH_VERIFY:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = AuthVerify(decode_auth_verify(pb))
			}
		case PAYLOAD_AUTH_NEW:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = AuthNew(decode_auth_new(pb))
			}
		case PAYLOAD_COMMAND:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = Command(decode_command(pb))
			}
		case PAYLOAD_COMMANDS:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = Commands(decode_commands(pb))
			}
		case PAYLOAD_MESSAGE_PRE_PROCESS:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = MessagePreProcess(decode_message_pre_process(pb))
			}
		case PAYLOAD_MESSAGE_IN_PROCESS:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = MessageInProcess(decode_message_in_process(pb))
			}
		case PAYLOAD_MESSAGE_POST_PROCESS:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = MessagePostProcess(decode_message_post_process(pb))
			}
		case PAYLOAD_TIMELINE_EVENT:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = TimelineEvent(decode_timeline_event(pb))
			}
		case PAYLOAD_USER_DATA:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = UserData(decode_user_data(pb))
			}
		case PAYLOAD_SHUTDOWN:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = Shutdown(decode_shutdown(pb))
			}
		case PAYLOAD_LOG:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = Log(decode_log(pb))
			}
		case PAYLOAD_ERR:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = Err(decode_err(pb))
			}
		case PAYLOAD_SEND_TO_PLATFORMS:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = SendToPlatforms(decode_send_to_platforms(pb))
			}
		case PAYLOAD_MESSAGE_ACK:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = MessageAck(decode_message_ack(pb))
			}
		case PAYLOAD_DATABASE_QUERY:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = DatabaseQuery(decode_database_query(pb))
			}
		case PAYLOAD_DATABASE_QUERY_RESULT:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = DatabaseQueryResult(decode_database_query_result(pb))
			}
		case PAYLOAD_MODULE_CONTROL:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = ModuleControl(decode_module_control(pb))
			}
		case PAYLOAD_MODULE_CONTROL_RESULT:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = ModuleControlResult(decode_module_control_result(pb))
			}
		case PAYLOAD_PROMPT:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = Prompt(decode_prompt(pb))
			}
		case PAYLOAD_PROMPT_RESPONSE:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = PromptResponse(decode_prompt_response(pb))
			}
		case PAYLOAD_AUDIT_FLAG:
			if pb, ok := read_msg_field(&r, wire); ok {
				c.payload = AuditFlag(decode_audit_flag(pb))
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

// Maps a Payload union member to its Container oneof field number.
payload_field_number :: proc(p: Payload) -> int {
	switch v in p {
	case ConnectionRequest:        return PAYLOAD_CONNECTION_REQUEST
	case ConnectionRequestReturn:  return PAYLOAD_CONNECTION_REQUEST_RETURN
	case AuthVerify:               return PAYLOAD_AUTH_VERIFY
	case AuthNew:                  return PAYLOAD_AUTH_NEW
	case Command:                  return PAYLOAD_COMMAND
	case Commands:                 return PAYLOAD_COMMANDS
	case MessagePreProcess:        return PAYLOAD_MESSAGE_PRE_PROCESS
	case MessageInProcess:         return PAYLOAD_MESSAGE_IN_PROCESS
	case MessagePostProcess:       return PAYLOAD_MESSAGE_POST_PROCESS
	case TimelineEvent:            return PAYLOAD_TIMELINE_EVENT
	case UserData:                 return PAYLOAD_USER_DATA
	case Shutdown:                 return PAYLOAD_SHUTDOWN
	case Log:                      return PAYLOAD_LOG
	case Err:                      return PAYLOAD_ERR
	case SendToPlatforms:          return PAYLOAD_SEND_TO_PLATFORMS
	case MessageAck:               return PAYLOAD_MESSAGE_ACK
	case DatabaseQuery:            return PAYLOAD_DATABASE_QUERY
	case DatabaseQueryResult:      return PAYLOAD_DATABASE_QUERY_RESULT
	case ModuleControl:            return PAYLOAD_MODULE_CONTROL
	case ModuleControlResult:      return PAYLOAD_MODULE_CONTROL_RESULT
	case Prompt:                   return PAYLOAD_PROMPT
	case PromptResponse:           return PAYLOAD_PROMPT_RESPONSE
	case AuditFlag:                return PAYLOAD_AUDIT_FLAG
	}
	return 0
}

// Maps a Payload union member to its client name ("" if none).
payload_name :: proc(p: Payload) -> string {
	switch v in p {
	case ConnectionRequest:        return "connectionRequest"
	case ConnectionRequestReturn:  return "connectionRequestReturn"
	case AuthVerify:               return "authVerify"
	case AuthNew:                  return "authNew"
	case Command:                  return "commandPayload"
	case Commands:                 return "commandsPayload"
	case MessagePreProcess:        return "messagePreProcess"
	case MessageInProcess:         return "messageInProcess"
	case MessagePostProcess:       return "messagePostProcess"
	case TimelineEvent:            return "timelineEvent"
	case UserData:                 return "userData"
	case Shutdown:                 return "shutdown"
	case Log:                      return "log"
	case Err:                      return "err"
	case SendToPlatforms:          return "sendToPlatforms"
	case MessageAck:               return "messageAck"
	case DatabaseQuery:            return "databaseQuery"
	case DatabaseQueryResult:      return "databaseQueryResult"
	case ModuleControl:            return "moduleControl"
	case ModuleControlResult:      return "moduleControlResult"
	case Prompt:                   return "prompt"
	case PromptResponse:           return "promptResponse"
	case AuditFlag:                return "auditFlag"
	}
	return ""
}

// Encodes any oneof payload member to protobuf bytes.
encode_payload :: proc(p: Payload) -> [dynamic]u8 {
	switch v in p {
	case ConnectionRequest:       return encode_connection_request(v)
	case ConnectionRequestReturn: return encode_connection_request_return(v)
	case AuthVerify:              return encode_auth_verify(v)
	case AuthNew:                 return encode_auth_new(v)
	case Command:                 return encode_command(v)
	case Commands:                return encode_commands(v)
	case MessagePreProcess:       return encode_message_pre_process(v)
	case MessageInProcess:        return encode_message_in_process(v)
	case MessagePostProcess:      return encode_message_post_process(v)
	case TimelineEvent:           return encode_timeline_event(v)
	case UserData:                return encode_user_data(v)
	case Shutdown:                return encode_shutdown(v)
	case Log:                     return encode_log(v)
	case Err:                     return encode_err(v)
	case SendToPlatforms:         return encode_send_to_platforms(v)
	case MessageAck:              return encode_message_ack(v)
	case DatabaseQuery:           return encode_database_query(v)
	case DatabaseQueryResult:     return encode_database_query_result(v)
	case ModuleControl:           return encode_module_control(v)
	case ModuleControlResult:     return encode_module_control_result(v)
	case Prompt:                  return encode_prompt(v)
	case PromptResponse:          return encode_prompt_response(v)
	case AuditFlag:               return encode_audit_flag(v)
	}
	return nil
}

// ============================================================================
// Wire primitives
// ============================================================================

Reader :: struct {
	b:   []byte,
	pos: int,
}

// --- encode ----------------------------------------------------------------

put_varint :: proc(w: ^[dynamic]u8, v: u64) {
	x := v
	for x >= 0x80 {
		append(w, u8(x & 0x7F) | 0x80)
		x >>= 7
	}
	append(w, u8(x))
}

put_key :: proc(w: ^[dynamic]u8, field_no: int, wire: int) {
	put_varint(w, u64(field_no << 3 | wire))
}

put_varint_field :: proc(w: ^[dynamic]u8, field_no: int, v: u64) {
	put_key(w, field_no, 0)
	put_varint(w, v)
}

put_i32_field :: proc(w: ^[dynamic]u8, field_no: int, v: i32) {
	if v != 0 {
		put_varint_field(w, field_no, u64(v))
	}
}

put_u32_field :: proc(w: ^[dynamic]u8, field_no: int, v: u32) {
	if v != 0 {
		put_varint_field(w, field_no, u64(v))
	}
}

put_bool_field :: proc(w: ^[dynamic]u8, field_no: int, v: bool) {
	if v {
		put_varint_field(w, field_no, 1)
	}
}

put_string_field :: proc(w: ^[dynamic]u8, field_no: int, s: string) {
	if len(s) > 0 {
		put_key(w, field_no, 2)
		put_varint(w, u64(len(s)))
		append(w, ..transmute([]u8)s)
	}
}

put_bytes_field :: proc(w: ^[dynamic]u8, field_no: int, b: []byte) {
	if len(b) > 0 {
		put_key(w, field_no, 2)
		put_varint(w, u64(len(b)))
		append(w, ..b)
	}
}

put_message_field :: proc(w: ^[dynamic]u8, field_no: int, msg: []byte) {
	put_key(w, field_no, 2)
	put_varint(w, u64(len(msg)))
	append(w, ..msg)
}

put_float_field :: proc(w: ^[dynamic]u8, field_no: int, f: f32) {
	if f != 0 {
		put_key(w, field_no, 5)
		bits := transmute(u32)f
		append(w, u8(bits & 0xFF), u8((bits >> 8) & 0xFF), u8((bits >> 16) & 0xFF), u8((bits >> 24) & 0xFF))
	}
}

put_repeated_string_field :: proc(w: ^[dynamic]u8, field_no: int, strs: []string) {
	for s in strs {
		put_string_field(w, field_no, s)
	}
}

put_map_string_string_field :: proc(w: ^[dynamic]u8, field_no: int, m: map[string]string) {
	for k, v in m {
		entry: [dynamic]u8
		put_string_field(&entry, 1, k)
		put_string_field(&entry, 2, v)
		put_message_field(w, field_no, entry[:])
		delete(entry)
	}
}

// --- decode ----------------------------------------------------------------

read_varint :: proc(r: ^Reader) -> (v: u64, ok: bool) {
	shift: u32 = 0
	for i in 0 ..< 10 {
		if r.pos >= len(r.b) {
			return 0, false
		}
		b := r.b[r.pos]
		r.pos += 1
		v |= u64(b & 0x7F) << shift
		if b & 0x80 == 0 {
			return v, true
		}
		shift += 7
	}
	return 0, false
}

read_bytes :: proc(r: ^Reader) -> (out: []byte, ok: bool) {
	n, ok2 := read_varint(r)
	if !ok2 {
		return nil, false
	}
	start := r.pos
	if n > u64(len(r.b) - start) {
		r.pos = len(r.b)
		return nil, false
	}
	r.pos += int(n)
	return r.b[start:r.pos], true
}

read_string :: proc(r: ^Reader) -> (s: string, ok: bool) {
	b, ok2 := read_bytes(r)
	if !ok2 {
		return "", false
	}
	return string(b), true
}

read_float :: proc(r: ^Reader) -> (f: f32, ok: bool) {
	if r.pos + 4 > len(r.b) {
		return 0, false
	}
	bits := u32(r.b[r.pos]) |
		u32(r.b[r.pos + 1]) << 8 |
		u32(r.b[r.pos + 2]) << 16 |
		u32(r.b[r.pos + 3]) << 24
	r.pos += 4
	return transmute(f32)bits, true
}

skip_field :: proc(r: ^Reader, wire: int) {
	switch wire {
	case 0:
		read_varint(r)
	case 1:
		r.pos = min(r.pos + 8, len(r.b))
	case 2:
		if n, ok := read_varint(r); ok {
			r.pos = min(r.pos + int(n), len(r.b))
		}
	case 5:
		r.pos = min(r.pos + 4, len(r.b))
	case:
		r.pos = len(r.b)
	}
}

read_varint_field :: proc(r: ^Reader, wire: int) -> (u64, bool) {
	if wire != 0 {
		skip_field(r, wire)
		return 0, false
	}
	return read_varint(r)
}

read_string_field :: proc(r: ^Reader, wire: int) -> (string, bool) {
	if wire != 2 {
		skip_field(r, wire)
		return "", false
	}
	return read_string(r)
}

read_msg_field :: proc(r: ^Reader, wire: int) -> ([]byte, bool) {
	if wire != 2 {
		skip_field(r, wire)
		return nil, false
	}
	return read_bytes(r)
}

read_float_field :: proc(r: ^Reader, wire: int) -> (f32, bool) {
	if wire != 5 {
		skip_field(r, wire)
		return 0, false
	}
	return read_float(r)
}

// ============================================================================
// Per-message codecs
// ============================================================================

encode_auth_new :: proc(m: AuthNew) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.new_auth)
	return w
}

decode_auth_new :: proc(b: []byte) -> (m: AuthNew) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		switch int(key >> 3) {
		case 1:
			if s, ok := read_string_field(&r, int(key & 7)); ok {
				m.new_auth = s
			}
		case:
			skip_field(&r, int(key & 7))
		}
	}
	return
}

encode_auth_verify :: proc(m: AuthVerify) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.cur_auth)
	return w
}

decode_auth_verify :: proc(b: []byte) -> (m: AuthVerify) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		switch int(key >> 3) {
		case 1:
			if s, ok := read_string_field(&r, int(key & 7)); ok {
				m.cur_auth = s
			}
		case:
			skip_field(&r, int(key & 7))
		}
	}
	return
}

encode_ban :: proc(m: Ban) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.commender_uuid7)
	put_string_field(&w, 2, m.commendee_uuid7)
	put_bool_field(&w, 3, m.unbanned)
	put_string_field(&w, 4, m.reason)
	put_string_field(&w, 5, m.raw_message)
	put_repeated_string_field(&w, 6, m.appeals[:])
	return w
}

decode_ban :: proc(b: []byte) -> (m: Ban) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.commender_uuid7 = s
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.commendee_uuid7 = s
			}
		case 3:
			if v, ok := read_varint_field(&r, wire); ok {
				m.unbanned = v != 0
			}
		case 4:
			if s, ok := read_string_field(&r, wire); ok {
				m.reason = s
			}
		case 5:
			if s, ok := read_string_field(&r, wire); ok {
				m.raw_message = s
			}
		case 6:
			if wire == 2 {
				if s, ok := read_string(&r); ok {
					append(&m.appeals, s)
				}
			} else {
				skip_field(&r, wire)
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_flag :: proc(m: Flag) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.flag_name)
	put_string_field(&w, 2, m.flag_description)
	put_i32_field(&w, 3, m.limiting_type)
	put_float_field(&w, 4, m.min_val)
	put_float_field(&w, 5, m.max_val)
	put_repeated_string_field(&w, 6, m.options[:])
	return w
}

decode_flag :: proc(b: []byte) -> (m: Flag) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.flag_name = s
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.flag_description = s
			}
		case 3:
			if v, ok := read_varint_field(&r, wire); ok {
				m.limiting_type = i32(v)
			}
		case 4:
			if f, ok := read_float_field(&r, wire); ok {
				m.min_val = f
			}
		case 5:
			if f, ok := read_float_field(&r, wire); ok {
				m.max_val = f
			}
		case 6:
			if wire == 2 {
				if s, ok := read_string(&r); ok {
					append(&m.options, s)
				}
			} else {
				skip_field(&r, wire)
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_command :: proc(m: Command) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.command_name)
	put_string_field(&w, 2, m.command_flag)
	put_string_field(&w, 3, m.command_description)
	for f in m.command_flags {
		inner := encode_flag(f)
		defer delete(inner)
		put_message_field(&w, 4, inner[:])
	}
	return w
}

decode_command :: proc(b: []byte) -> (m: Command) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.command_name = s
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.command_flag = s
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.command_description = s
			}
		case 4:
			if wire == 2 {
				if pb, ok := read_bytes(&r); ok {
					append(&m.command_flags, decode_flag(pb))
				}
			} else {
				skip_field(&r, wire)
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_commands :: proc(m: Commands) -> [dynamic]u8 {
	w: [dynamic]u8
	for cmd in m.commands {
		inner := encode_command(cmd)
		defer delete(inner)
		put_message_field(&w, 1, inner[:])
	}
	return w
}

decode_commands :: proc(b: []byte) -> (m: Commands) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		if field_no == 1 && wire == 2 {
			if pb, ok := read_bytes(&r); ok {
				append(&m.commands, decode_command(pb))
			}
		} else {
			skip_field(&r, wire)
		}
	}
	return
}

encode_commendation :: proc(m: Commendation) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.commender_uuid7)
	put_string_field(&w, 2, m.commendee_uuid7)
	put_string_field(&w, 3, m.raw_message)
	put_string_field(&w, 4, m.reason)
	return w
}

decode_commendation :: proc(b: []byte) -> (m: Commendation) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.commender_uuid7 = s
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.commendee_uuid7 = s
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.raw_message = s
			}
		case 4:
			if s, ok := read_string_field(&r, wire); ok {
				m.reason = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_reprimand :: proc(m: Reprimand) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.commender_uuid7)
	put_string_field(&w, 2, m.commendee_uuid7)
	put_string_field(&w, 3, m.raw_message)
	put_string_field(&w, 4, m.reason)
	return w
}

decode_reprimand :: proc(b: []byte) -> (m: Reprimand) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.commender_uuid7 = s
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.commendee_uuid7 = s
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.raw_message = s
			}
		case 4:
			if s, ok := read_string_field(&r, wire); ok {
				m.reason = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_user_styling_template :: proc(m: UserStylingTemplate) -> [dynamic]u8 {
	w: [dynamic]u8
	put_map_string_string_field(&w, 1, m.css_properties)
	return w
}

decode_user_styling_template :: proc(b: []byte) -> (m: UserStylingTemplate) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		if field_no == 1 && wire == 2 {
			if pb, ok := read_bytes(&r); ok {
				k, v := decode_map_entry(pb)
				if m.css_properties == nil {
					m.css_properties = make(map[string]string)
				}
				m.css_properties[k] = v
			}
		} else {
			skip_field(&r, wire)
		}
	}
	return
}

decode_map_entry :: proc(b: []byte) -> (k: string, v: string) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		if field_no == 1 {
			if s, ok := read_string_field(&r, wire); ok {
				k = s
			}
		} else if field_no == 2 {
			if s, ok := read_string_field(&r, wire); ok {
				v = s
			}
		} else {
			skip_field(&r, wire)
		}
	}
	return
}

encode_user_data :: proc(m: UserData) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.uuid)
	put_string_field(&w, 2, m.username)
	put_bool_field(&w, 3, m.is_sponsor)
	put_bool_field(&w, 4, m.is_moderator)
	put_bool_field(&w, 5, m.is_admin)
	put_bool_field(&w, 6, m.is_owner)
	for b in m.bans {
		inner := encode_ban(b)
		defer delete(inner)
		put_message_field(&w, 7, inner[:])
	}
	for c in m.commendations {
		inner := encode_commendation(c)
		defer delete(inner)
		put_message_field(&w, 8, inner[:])
	}
	if m.styling != nil {
		inner := encode_user_styling_template(m.styling^)
		defer delete(inner)
		put_message_field(&w, 9, inner[:])
	}
	put_map_string_string_field(&w, 10, m.platform_ids)
	return w
}

decode_user_data :: proc(b: []byte) -> (m: UserData) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.uuid = s
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.username = s
			}
		case 3:
			if v, ok := read_varint_field(&r, wire); ok {
				m.is_sponsor = v != 0
			}
		case 4:
			if v, ok := read_varint_field(&r, wire); ok {
				m.is_moderator = v != 0
			}
		case 5:
			if v, ok := read_varint_field(&r, wire); ok {
				m.is_admin = v != 0
			}
		case 6:
			if v, ok := read_varint_field(&r, wire); ok {
				m.is_owner = v != 0
			}
		case 7:
			if wire == 2 {
				if pb, ok := read_bytes(&r); ok {
					append(&m.bans, decode_ban(pb))
				}
			} else {
				skip_field(&r, wire)
			}
		case 8:
			if wire == 2 {
				if pb, ok := read_bytes(&r); ok {
					append(&m.commendations, decode_commendation(pb))
				}
			} else {
				skip_field(&r, wire)
			}
		case 9:
			if wire == 2 {
				if pb, ok := read_bytes(&r); ok {
					m.styling = new(UserStylingTemplate)
					m.styling^ = decode_user_styling_template(pb)
				}
			} else {
				skip_field(&r, wire)
			}
		case 10:
			if wire == 2 {
				if pb, ok := read_bytes(&r); ok {
					k, v := decode_map_entry(pb)
					if m.platform_ids == nil {
						m.platform_ids = make(map[string]string)
					}
					m.platform_ids[k] = v
				}
			} else {
				skip_field(&r, wire)
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_connection_request :: proc(m: ConnectionRequest) -> [dynamic]u8 {
	w: [dynamic]u8
	put_i32_field(&w, 1, m.pin)
	put_i32_field(&w, 2, m.process_position)
	put_u32_field(&w, 3, m.priority)
	put_string_field(&w, 4, m.module_instance_uuid7)
	return w
}

decode_connection_request :: proc(b: []byte) -> (m: ConnectionRequest) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if v, ok := read_varint_field(&r, wire); ok {
				m.pin = i32(v)
			}
		case 2:
			if v, ok := read_varint_field(&r, wire); ok {
				m.process_position = i32(v)
			}
		case 3:
			if v, ok := read_varint_field(&r, wire); ok {
				m.priority = u32(v)
			}
		case 4:
			if s, ok := read_string_field(&r, wire); ok {
				m.module_instance_uuid7 = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_connection_request_return :: proc(m: ConnectionRequestReturn) -> [dynamic]u8 {
	w: [dynamic]u8
	put_u32_field(&w, 1, m.new_port)
	put_string_field(&w, 2, m.module_instance_uuid7)
	return w
}

decode_connection_request_return :: proc(b: []byte) -> (m: ConnectionRequestReturn) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if v, ok := read_varint_field(&r, wire); ok {
				m.new_port = u32(v)
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.module_instance_uuid7 = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_err :: proc(m: Err) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.log)
	put_bytes_field(&w, 2, m.blob)
	put_string_field(&w, 3, m.trace)
	return w
}

decode_err :: proc(b: []byte) -> (m: Err) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.log = s
			}
		case 2:
			if pb, ok := read_msg_field(&r, wire); ok {
				m.blob = pb
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.trace = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_log :: proc(m: Log) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.log)
	put_bytes_field(&w, 2, m.blob)
	return w
}

decode_log :: proc(b: []byte) -> (m: Log) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.log = s
			}
		case 2:
			if pb, ok := read_msg_field(&r, wire); ok {
				m.blob = pb
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_shutdown :: proc(m: Shutdown) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.reason)
	return w
}

decode_shutdown :: proc(b: []byte) -> (m: Shutdown) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.reason = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_send_to_platforms :: proc(m: SendToPlatforms) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.msg)
	put_i32_field(&w, 2, m.level)
	put_string_field(&w, 3, m.module_uuid7)
	put_string_field(&w, 4, m.pid)
	put_string_field(&w, 5, m.platform)
	put_string_field(&w, 6, m.actor_platform)
	put_string_field(&w, 7, m.actor_handle)
	put_string_field(&w, 8, m.actor_uuid7)
	return w
}

decode_send_to_platforms :: proc(b: []byte) -> (m: SendToPlatforms) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.msg = s
			}
		case 2:
			if v, ok := read_varint_field(&r, wire); ok {
				m.level = i32(v)
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.module_uuid7 = s
			}
		case 4:
			if s, ok := read_string_field(&r, wire); ok {
				m.pid = s
			}
		case 5:
			if s, ok := read_string_field(&r, wire); ok {
				m.platform = s
			}
		case 6:
			if s, ok := read_string_field(&r, wire); ok {
				m.actor_platform = s
			}
		case 7:
			if s, ok := read_string_field(&r, wire); ok {
				m.actor_handle = s
			}
		case 8:
			if s, ok := read_string_field(&r, wire); ok {
				m.actor_uuid7 = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_message_ack :: proc(m: MessageAck) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.message_uuid7)
	return w
}

decode_message_ack :: proc(b: []byte) -> (m: MessageAck) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.message_uuid7 = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_database_query :: proc(m: DatabaseQuery) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.query_id)
	put_string_field(&w, 2, m.sql)
	put_repeated_string_field(&w, 3, m.params[:])
	return w
}

decode_database_query :: proc(b: []byte) -> (m: DatabaseQuery) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.query_id = s
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.sql = s
			}
		case 3:
			if wire == 2 {
				if s, ok := read_string(&r); ok {
					append(&m.params, s)
				}
			} else {
				skip_field(&r, wire)
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_database_query_result :: proc(m: DatabaseQueryResult) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.query_id)
	put_bool_field(&w, 2, m.success)
	put_string_field(&w, 3, m.error)
	put_bytes_field(&w, 4, m.result_blob)
	return w
}

decode_database_query_result :: proc(b: []byte) -> (m: DatabaseQueryResult) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.query_id = s
			}
		case 2:
			if v, ok := read_varint_field(&r, wire); ok {
				m.success = v != 0
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.error = s
			}
		case 4:
			if pb, ok := read_msg_field(&r, wire); ok {
				m.result_blob = pb
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_module_control :: proc(m: ModuleControl) -> [dynamic]u8 {
	w: [dynamic]u8
	put_i32_field(&w, 1, m.action)
	put_string_field(&w, 2, m.module_name)
	put_bool_field(&w, 3, m.autostart)
	return w
}

decode_module_control :: proc(b: []byte) -> (m: ModuleControl) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if v, ok := read_varint_field(&r, wire); ok {
				m.action = i32(v)
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.module_name = s
			}
		case 3:
			if v, ok := read_varint_field(&r, wire); ok {
				m.autostart = v != 0
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_module_control_result :: proc(m: ModuleControlResult) -> [dynamic]u8 {
	w: [dynamic]u8
	put_bool_field(&w, 1, m.success)
	put_string_field(&w, 2, m.error)
	put_string_field(&w, 3, m.message)
	return w
}

decode_module_control_result :: proc(b: []byte) -> (m: ModuleControlResult) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if v, ok := read_varint_field(&r, wire); ok {
				m.success = v != 0
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.error = s
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.message = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_prompt :: proc(m: Prompt) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.prompt_id_uuid7)
	put_string_field(&w, 2, m.prompt)
	put_string_field(&w, 3, m.details)
	put_string_field(&w, 4, m.yes_dialog)
	put_string_field(&w, 5, m.no_dialog)
	put_u32_field(&w, 6, m.timeout)
	put_string_field(&w, 7, m.origin)
	put_string_field(&w, 8, m.origin_uuid7)
	put_string_field(&w, 9, m.instructions)
	put_string_field(&w, 10, m.link)
	put_string_field(&w, 11, m.input_label)
	put_i32_field(&w, 12, m.prompt_type)
	return w
}

decode_prompt :: proc(b: []byte) -> (m: Prompt) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.prompt_id_uuid7 = s
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.prompt = s
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.details = s
			}
		case 4:
			if s, ok := read_string_field(&r, wire); ok {
				m.yes_dialog = s
			}
		case 5:
			if s, ok := read_string_field(&r, wire); ok {
				m.no_dialog = s
			}
		case 6:
			if v, ok := read_varint_field(&r, wire); ok {
				m.timeout = u32(v)
			}
		case 7:
			if s, ok := read_string_field(&r, wire); ok {
				m.origin = s
			}
		case 8:
			if s, ok := read_string_field(&r, wire); ok {
				m.origin_uuid7 = s
			}
		case 9:
			if s, ok := read_string_field(&r, wire); ok {
				m.instructions = s
			}
		case 10:
			if s, ok := read_string_field(&r, wire); ok {
				m.link = s
			}
		case 11:
			if s, ok := read_string_field(&r, wire); ok {
				m.input_label = s
			}
		case 12:
			if v, ok := read_varint_field(&r, wire); ok {
				m.prompt_type = i32(v)
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_prompt_response :: proc(m: PromptResponse) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.prompt_id_uuid7)
	put_bool_field(&w, 2, m.accepted)
	put_string_field(&w, 3, m.reason)
	return w
}

decode_prompt_response :: proc(b: []byte) -> (m: PromptResponse) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.prompt_id_uuid7 = s
			}
		case 2:
			if v, ok := read_varint_field(&r, wire); ok {
				m.accepted = v != 0
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.reason = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_audit_flag :: proc(m: AuditFlag) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.message_uuid7)
	put_string_field(&w, 2, m.reason)
	put_string_field(&w, 3, m.origin)
	return w
}

decode_audit_flag :: proc(b: []byte) -> (m: AuditFlag) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.message_uuid7 = s
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.reason = s
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.origin = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_chat_message :: proc(m: ChatMessage) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.platform)
	put_bytes_field(&w, 2, m.raw_data)
	put_string_field(&w, 3, m.raw_message)
	put_string_field(&w, 4, m.user_uuid7)
	if m.command != nil {
		inner := encode_command(m.command^)
		defer delete(inner)
		put_message_field(&w, 5, inner[:])
	}
	if m.user_data != nil {
		inner := encode_user_data(m.user_data^)
		defer delete(inner)
		put_message_field(&w, 6, inner[:])
	}
	return w
}

decode_chat_message :: proc(b: []byte) -> (m: ChatMessage) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.platform = s
			}
		case 2:
			if pb, ok := read_msg_field(&r, wire); ok {
				m.raw_data = pb
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.raw_message = s
			}
		case 4:
			if s, ok := read_string_field(&r, wire); ok {
				m.user_uuid7 = s
			}
		case 5:
			if wire == 2 {
				if pb, ok := read_bytes(&r); ok {
					m.command = new(Command)
					m.command^ = decode_command(pb)
				}
			} else {
				skip_field(&r, wire)
			}
		case 6:
			if wire == 2 {
				if pb, ok := read_bytes(&r); ok {
					m.user_data = new(UserData)
					m.user_data^ = decode_user_data(pb)
				}
			} else {
				skip_field(&r, wire)
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_message_pre_process :: proc(m: MessagePreProcess) -> [dynamic]u8 {
	w: [dynamic]u8
	if m.raw_message != nil {
		inner := encode_chat_message(m.raw_message^)
		defer delete(inner)
		put_message_field(&w, 1, inner[:])
	}
	put_string_field(&w, 2, m.message_uuid7)
	put_bytes_field(&w, 3, m.audio)
	put_string_field(&w, 4, m.audio_type)
	return w
}

decode_message_pre_process :: proc(b: []byte) -> (m: MessagePreProcess) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if wire == 2 {
				if pb, ok := read_bytes(&r); ok {
					m.raw_message = new(ChatMessage)
					m.raw_message^ = decode_chat_message(pb)
				}
			} else {
				skip_field(&r, wire)
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.message_uuid7 = s
			}
		case 3:
			if pb, ok := read_msg_field(&r, wire); ok {
				m.audio = pb
			}
		case 4:
			if s, ok := read_string_field(&r, wire); ok {
				m.audio_type = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_message_in_process :: proc(m: MessageInProcess) -> [dynamic]u8 {
	w: [dynamic]u8
	if m.raw_message != nil {
		inner := encode_chat_message(m.raw_message^)
		defer delete(inner)
		put_message_field(&w, 1, inner[:])
	}
	put_string_field(&w, 2, m.processed_message)
	put_bool_field(&w, 3, m.abandon_message)
	put_string_field(&w, 4, m.message_uuid7)
	put_bytes_field(&w, 5, m.audio)
	put_string_field(&w, 6, m.audio_type)
	return w
}

decode_message_in_process :: proc(b: []byte) -> (m: MessageInProcess) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if wire == 2 {
				if pb, ok := read_bytes(&r); ok {
					m.raw_message = new(ChatMessage)
					m.raw_message^ = decode_chat_message(pb)
				}
			} else {
				skip_field(&r, wire)
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.processed_message = s
			}
		case 3:
			if v, ok := read_varint_field(&r, wire); ok {
				m.abandon_message = v != 0
			}
		case 4:
			if s, ok := read_string_field(&r, wire); ok {
				m.message_uuid7 = s
			}
		case 5:
			if pb, ok := read_msg_field(&r, wire); ok {
				m.audio = pb
			}
		case 6:
			if s, ok := read_string_field(&r, wire); ok {
				m.audio_type = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_message_post_process :: proc(m: MessagePostProcess) -> [dynamic]u8 {
	w: [dynamic]u8
	if m.raw_message != nil {
		inner := encode_chat_message(m.raw_message^)
		defer delete(inner)
		put_message_field(&w, 1, inner[:])
	}
	put_string_field(&w, 2, m.processed_message)
	put_string_field(&w, 3, m.message_uuid7)
	put_bytes_field(&w, 4, m.audio)
	put_string_field(&w, 5, m.audio_type)
	return w
}

decode_message_post_process :: proc(b: []byte) -> (m: MessagePostProcess) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if wire == 2 {
				if pb, ok := read_bytes(&r); ok {
					m.raw_message = new(ChatMessage)
					m.raw_message^ = decode_chat_message(pb)
				}
			} else {
				skip_field(&r, wire)
			}
		case 2:
			if s, ok := read_string_field(&r, wire); ok {
				m.processed_message = s
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.message_uuid7 = s
			}
		case 4:
			if pb, ok := read_msg_field(&r, wire); ok {
				m.audio = pb
			}
		case 5:
			if s, ok := read_string_field(&r, wire); ok {
				m.audio_type = s
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

encode_timeline_event :: proc(m: TimelineEvent) -> [dynamic]u8 {
	w: [dynamic]u8
	put_string_field(&w, 1, m.timeline_id_uuid7)
	put_i32_field(&w, 2, m.event_type)
	put_string_field(&w, 3, m.command_flag)
	put_bytes_field(&w, 4, m.data_blob)
	put_string_field(&w, 5, m.error_message)
	put_string_field(&w, 6, m.raw_flags)
	put_string_field(&w, 7, m.message_origin)
	put_string_field(&w, 8, m.stream_origin)
	put_string_field(&w, 9, m.raw_message)
	put_string_field(&w, 10, m.processed_message)
	put_string_field(&w, 11, m.user_uuid7)
	put_u32_field(&w, 12, m.version)
	return w
}

decode_timeline_event :: proc(b: []byte) -> (m: TimelineEvent) {
	r := Reader { b = b }
	for r.pos < len(r.b) {
		key, ok := read_varint(&r)
		if !ok {
			break
		}
		field_no := int(key >> 3)
		wire := int(key & 7)
		switch field_no {
		case 1:
			if s, ok := read_string_field(&r, wire); ok {
				m.timeline_id_uuid7 = s
			}
		case 2:
			if v, ok := read_varint_field(&r, wire); ok {
				m.event_type = i32(v)
			}
		case 3:
			if s, ok := read_string_field(&r, wire); ok {
				m.command_flag = s
			}
		case 4:
			if pb, ok := read_msg_field(&r, wire); ok {
				m.data_blob = pb
			}
		case 5:
			if s, ok := read_string_field(&r, wire); ok {
				m.error_message = s
			}
		case 6:
			if s, ok := read_string_field(&r, wire); ok {
				m.raw_flags = s
			}
		case 7:
			if s, ok := read_string_field(&r, wire); ok {
				m.message_origin = s
			}
		case 8:
			if s, ok := read_string_field(&r, wire); ok {
				m.stream_origin = s
			}
		case 9:
			if s, ok := read_string_field(&r, wire); ok {
				m.raw_message = s
			}
		case 10:
			if s, ok := read_string_field(&r, wire); ok {
				m.processed_message = s
			}
		case 11:
			if s, ok := read_string_field(&r, wire); ok {
				m.user_uuid7 = s
			}
		case 12:
			if v, ok := read_varint_field(&r, wire); ok {
				m.version = u32(v)
			}
		case:
			skip_field(&r, wire)
		}
	}
	return
}

// ============================================================================
// WebSocket transport (RFC 6455) over core:net TCP
// ============================================================================

// Parses "ws://host:port/path" into its parts.
parse_ws_url :: proc(url: string, host: ^string, port: ^int, path: ^string) -> bool {
	rest := url
	if strings.has_prefix(rest, "ws://") {
		rest = rest[len("ws://"):]
	} else if strings.has_prefix(rest, "wss://") {
		return false // TLS is out of scope for the native client
	} else {
		return false
	}
	slash := strings.index_byte(rest, '/')
	authority, p: string
	if slash < 0 {
		authority = rest
		p = "/"
	} else {
		authority = rest[:slash]
		p = rest[slash:]
	}
	if len(p) == 0 {
		p = "/"
	}
	colon := strings.last_index_byte(authority, ':')
	if colon < 0 {
		port^ = 9734
		host^ = authority
	} else {
		pn, ok := strconv.parse_int(authority[colon + 1:], 10)
		if !ok || pn <= 0 || pn > 65535 {
			return false
		}
		port^ = pn
		host^ = authority[:colon]
	}
	if host^ == "" {
		return false
	}
	path^ = p
	return true
}

// Opens a TCP socket to the client's host:port and performs the HTTP/1.1
// upgrade handshake (RFC 6455). On success c.socket is the live WebSocket.
ws_connect :: proc(c: ^Client, timeout: time.Duration) -> bool {
	sock, err := net.dial_tcp_from_hostname_with_port_override(c.host, c.port)
	if err != nil {
		c.last_error = fmt.aprintf("TCP dial to %s:%d failed", c.host, c.port)
		return false
	}
	c.socket = sock
	net.set_option(c.socket, .Receive_Timeout, timeout)
	net.set_option(c.socket, .TCP_Nodelay, true)

	key16: [16]byte
	crypto.rand_bytes(key16[:])
	key_b64, _ := base64.encode(key16[:])
	defer delete(key_b64)

	sb := strings.builder_make()
	defer strings.builder_destroy(&sb)
	strings.write_string(&sb, "GET ")
	strings.write_string(&sb, c.path)
	strings.write_string(&sb, " HTTP/1.1\r\n")
	strings.write_string(&sb, "Host: ")
	strings.write_string(&sb, fmt.aprintf("%s:%d", c.host, c.port))
	strings.write_string(&sb, "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n")
	strings.write_string(&sb, "Sec-WebSocket-Key: ")
	strings.write_string(&sb, key_b64)
	strings.write_string(&sb, "\r\nSec-WebSocket-Version: 13\r\nOrigin: cockatiel-odin\r\n\r\n")

	req := strings.to_string(sb)
	if _, serr := net.send_tcp(c.socket, transmute([]u8)req); serr != .None {
		c.last_error = fmt.aprintf("WebSocket upgrade send failed: %v", serr)
		ws_close_socket(c)
		return false
	}

	// Read the HTTP response headers.
	headers := ws_read_headers(c)
	if headers == nil {
		ws_close_socket(c)
		return false
	}
	defer delete(headers)

	status_line := headers[0]
	if !strings.contains(status_line, " 101 ") {
		c.last_error = fmt.aprintf("WebSocket upgrade rejected: %s", status_line)
		ws_close_socket(c)
		return false
	}
	has_upgrade := false
	accept := ""
	for h in headers[1:] {
		lower := strings.to_lower(h)
		trim := strings.trim_space(lower)
		if strings.has_prefix(trim, "upgrade:") && strings.contains(trim, "websocket") {
			has_upgrade = true
		}
		if strings.has_prefix(trim, "sec-websocket-accept:") {
			// take the value from the ORIGINAL header (not the lowercased copy)
			orig := strings.trim_space(h)
			accept = strings.trim_space(orig[len("sec-websocket-accept:"):])
		}
	}
	if !has_upgrade {
		c.last_error = "WebSocket upgrade missing 'Upgrade: websocket' header"
		ws_close_socket(c)
		return false
	}
	if accept != "" {
		expected := ws_accept_value(key_b64)
		defer delete(expected)
		if accept != expected {
			c.last_error = "WebSocket Sec-WebSocket-Accept mismatch"
			ws_close_socket(c)
			return false
		}
	}
	return true
}

// Reads HTTP response header lines until the blank line. Returns the header
// lines (status line first, cloned so they own their memory), or nil on
// error/timeout. The caller must delete() the returned slice.
ws_read_headers :: proc(c: ^Client) -> []string {
	buf: [dynamic]u8
	defer delete(buf)
	for len(buf) < 16384 {
		one: [1]u8
		if !read_exact(c, one[:], timeout_is_error = true) {
			return nil
		}
		append(&buf, one[0])
		if len(buf) >= 4 &&
			buf[len(buf) - 4] == '\r' &&
			buf[len(buf) - 3] == '\n' &&
			buf[len(buf) - 2] == '\r' &&
			buf[len(buf) - 1] == '\n' {
			text := string(buf[:])
			lines := strings.split_lines(text)
			defer delete(lines)
			out: [dynamic]string
			for l in lines {
				if len(l) > 0 {
					append(&out, strings.clone(l))
				}
			}
			return out[:]
		}
	}
	return nil
}

// Sends a single RFC 6455 frame (client frames are always masked).
ws_send_frame :: proc(c: ^Client, opcode: u8, payload: []byte) -> bool {
	key: [4]byte
	crypto.rand_bytes(key[:])

	ln := len(payload)
	header: [10]byte
	header[0] = 0x80 | (opcode & 0x0F) // FIN + opcode
	idx := 1
	if ln < 126 {
		header[1] = 0x80 | u8(ln) // MASK + 7-bit length
		idx = 2
	} else if ln <= 0xFFFF {
		header[1] = 0x80 | 126
		header[2] = u8(ln >> 8)
		header[3] = u8(ln & 0xFF)
		idx = 4
	} else {
		header[1] = 0x80 | 127
		l := u64(ln)
		for i in 0 ..< 8 {
			header[2 + i] = u8((l >> u32(8 * (7 - i))) & 0xFF)
		}
		idx = 10
	}

	frame := make([]u8, idx + ln + 4)
	defer delete(frame)
	copy(frame[:idx], header[:idx])
	copy(frame[idx:idx + 4], key[:])
	idx += 4
	for i in 0 ..< ln {
		frame[idx + i] = payload[i] ~ key[i & 3]
	}

	if _, serr := net.send_tcp(c.socket, frame); serr != .None {
		c.last_error = fmt.aprintf("WebSocket send failed: %v", serr)
		return false
	}
	return true
}

// Reads exactly one WebSocket frame from the socket. Handles ping (answers
// pong) and close (answers close) internally. Returns the payload (only valid
// for non-control data frames), closed=true when the peer closed, ok=false on
// error. The returned payload slice is OWNED by the caller and must be
// delete()d after the frame has been decoded and dispatched (decoded strings
// alias it). For control frames the payload is nil.
ws_read_frame :: proc(c: ^Client, timeout_is_error: bool = false) -> (opcode: u8, payload: []byte, closed: bool, ok: bool) {
	for {
		hdr: [2]u8
		if !read_exact(c, hdr[:], timeout_is_error) {
			return 0, nil, true, false
		}
		fin := hdr[0] & 0x80 != 0
		op := hdr[0] & 0x0F
		masked := hdr[1] & 0x80 != 0
		length := u64(hdr[1] & 0x7F)

		if length == 126 {
			ext: [2]u8
			if !read_exact(c, ext[:], timeout_is_error) {
				return 0, nil, true, false
			}
			length = u64(ext[0]) << 8 | u64(ext[1])
		} else if length == 127 {
			ext: [8]u8
			if !read_exact(c, ext[:], timeout_is_error) {
				return 0, nil, true, false
			}
			length = 0
			for i in 0 ..< 8 {
				length = length << 8 | u64(ext[i])
			}
		}
		if length > MAX_FRAME_SIZE {
			c.last_error = fmt.aprintf("frame too large: %d", length)
			return 0, nil, true, false
		}

		mask_key: [4]u8
		if masked {
			if !read_exact(c, mask_key[:], timeout_is_error) {
				return 0, nil, true, false
			}
		}

		buf := make([]u8, length)
		if length > 0 {
			if !read_exact(c, buf, timeout_is_error) {
				delete(buf)
				return 0, nil, true, false
			}
			if masked {
				for i in 0 ..< int(length) {
					buf[i] = buf[i] ~ mask_key[i & 3]
				}
			}
		}

		switch op {
		case OP_PING:
			ws_send_frame(c, OP_PONG, buf)
			delete(buf)
			continue
		case OP_CLOSE:
			ws_send_frame(c, OP_CLOSE, buf)
			delete(buf)
			return OP_CLOSE, nil, true, true
		case OP_CONTINUATION, OP_TEXT, OP_BINARY:
			return op, buf, false, true
		case:
			delete(buf)
			continue
		}
	}
}

// Reads exactly len(buf) bytes (looping). On receive timeout, retries until
// client.stop (for the receive loop) or bails when timeout_is_error (handshake).
read_exact :: proc(c: ^Client, buf: []byte, timeout_is_error: bool) -> bool {
	off := 0
	for off < len(buf) {
		if c.stop && !timeout_is_error {
			return false
		}
		n, err := net.recv_tcp(c.socket, buf[off:])
		if err == .None {
			if n == 0 {
				c.connected = false
				return false // graceful close
			}
			off += n
			continue
		}
		if err == .Timeout {
			if timeout_is_error {
				c.last_error = "receive timed out"
				return false
			}
			continue
		}
		c.last_error = fmt.aprintf("recv error: %v", err)
		return false
	}
	return true
}

// Sends a best-effort WS close frame and closes the TCP socket.
ws_close_socket :: proc(c: ^Client) {
	ws_send_frame(c, OP_CLOSE, nil)
	net.close(c.socket)
	c.connected = false
}

// RFC 6455 Sec-WebSocket-Accept = base64( SHA1(key + GUID) ).
ws_accept_value :: proc(key: string) -> string {
	concat := fmt.aprintf("%s%s", key, WS_GUID)
	defer delete(concat)
	digest := sha1(transmute([]u8)concat)
	enc, _ := base64.encode(digest[:])
	return enc
}

// ============================================================================
// SHA-1 (FIPS 180-4) — needed for the WebSocket handshake; hand-rolled to
// keep the client fully native (no external digest package).
// ============================================================================

sha1 :: proc(data: []byte) -> (out: [20]byte) {
	msg := data

	bit_len := u64(len(msg)) * 8
	// padding: 0x80, then zeros, then 8-byte big-endian bit length
	pad_len := 0
	mod := len(msg) % 64
	if mod < 56 {
		pad_len = 56 - mod
	} else {
		pad_len = 120 - mod
	}

	buf := make([]u8, len(msg) + pad_len + 8)
	defer delete(buf)
	copy(buf, msg)
	buf[len(msg)] = 0x80
	for i := 0; i < 8; i += 1 {
		buf[len(buf) - 1 - i] = u8(bit_len >> (8 * u32(i)))
	}

	h0: u32 = 0x67452301
	h1: u32 = 0xEFCDAB89
	h2: u32 = 0x98BADCFE
	h3: u32 = 0x10325476
	h4: u32 = 0xC3D2E1F0

	w: [80]u32
	for chunk := 0; chunk < len(buf); chunk += 64 {
		for i := 0; i < 16; i += 1 {
			j := chunk + i * 4
			w[i] = u32(buf[j]) << 24 | u32(buf[j + 1]) << 16 | u32(buf[j + 2]) << 8 | u32(buf[j + 3])
		}
		for i := 16; i < 80; i += 1 {
			w[i] = rotate_left(w[i - 3] ~ w[i - 8] ~ w[i - 14] ~ w[i - 16], 1)
		}
		a := h0
		b := h1
		cc := h2
		d := h3
		e := h4
		for i := 0; i < 80; i += 1 {
			f, k: u32
			switch {
			case i < 20:
				f = (b & cc) | (~b & d)
				k = 0x5A827999
			case i < 40:
				f = b ~ cc ~ d
				k = 0x6ED9EBA1
			case i < 60:
				f = (b & cc) | (b & d) | (cc & d)
				k = 0x8F1BBCDC
			case:
				f = b ~ cc ~ d
				k = 0xCA62C1D6
			}
			temp := rotate_left(a, 5) + f + e + k + w[i]
			e = d
			d = cc
			cc = rotate_left(b, 30)
			b = a
			a = temp
		}
		h0 += a
		h1 += b
		h2 += cc
		h3 += d
		h4 += e
	}

	hs := [5]u32 { h0, h1, h2, h3, h4 }
	for i := 0; i < 5; i += 1 {
		v := hs[i]
		out[i * 4 + 0] = u8(v >> 24)
		out[i * 4 + 1] = u8(v >> 16)
		out[i * 4 + 2] = u8(v >> 8)
		out[i * 4 + 3] = u8(v)
	}
	return
}

rotate_left :: proc(x: u32, n: u32) -> u32 {
	return (x << n) | (x >> (32 - n))
}

// ============================================================================
// UUIDv7 (RFC 9562) — time-ordered, 36-char lowercase dashed form.
// ============================================================================
uuid7 :: proc() -> string {
	rnd: [10]byte
	crypto.rand_bytes(rnd[:])

	ms := u64(time.time_to_unix_nano(time.now()) / i64(time.Millisecond)) & 0xFFFFFFFFFFFF

	b: [16]byte
	for i := 0; i < 6; i += 1 {
		b[5 - i] = u8(ms >> u32(8 * i))
	}
	b[6] = (0x70) | (rnd[0] & 0x0F)      // version 7
	b[7] = (0x80) | (rnd[1] & 0x3F)      // variant 10xx
	copy(b[8:], rnd[2:])

	hex_digits := "0123456789abcdef"
	out: [36]byte
	hi := 0
	for i := 0; i < 16; i += 1 {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out[hi] = '-'
			hi += 1
		}
		out[hi] = hex_digits[b[i] >> 4]
		out[hi + 1] = hex_digits[b[i] & 0x0F]
		hi += 2
	}
	// clone: the fixed array is stack-allocated and dies with this frame.
	return strings.clone(string(out[:]))
}