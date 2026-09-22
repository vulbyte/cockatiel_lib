-- ============================================================================
-- cockatiel_lib.lua — Cockatiel chat-engine client for LuaJIT 2.1+
--
-- One self-contained module implementing CLIENT_CONTRACT.md:
--   * hand-rolled proto3 wire codec (pure Lua, 32-bit split 64-bit varints)
--     covering the ENTIRE `Container` + all 23 payload messages
--   * RFC6455 WebSocket transport on raw libc sockets via FFI (no deps)
--   * single-connection PIN -> JWT auth (no two-phase / port hop)
--   * automatic AuthVerify liveness answers inside the receive path
--   * reconnect carrying the stored JWT
--   * PIN precedence: COCKATIEL_PIN env -> opts.pin
--   * a time-ordered uuid7 generator
--
-- Runtime dependencies: LuaJIT + FFI only. No luarocks, no luasocket, no
-- cjson, no external protobuf/websocket libraries.
--
-- Import:  local Cockatiel = require("cockatiel_lib")
-- ============================================================================

local ffi = require("ffi")
local bit = require("bit")
local C = ffi.C

ffi.cdef[[
typedef unsigned char uint8_t;
typedef short int16_t;
typedef unsigned short uint16_t;
typedef int int32_t;
typedef unsigned int uint32_t;
typedef long ssize_t;
typedef struct FILE FILE;

struct timeval { long tv_sec; int tv_usec; };
struct fd_set { int fds_bits[32]; };
struct sockaddr_in { uint8_t sin_len; uint8_t sin_family; uint16_t sin_port; uint32_t sin_addr; char sin_zero[8]; };

int socket(int domain, int type, int protocol);
int connect(int fd, const struct sockaddr *addr, unsigned int addrlen);
ssize_t send(int fd, const void *buf, size_t n, int flags);
ssize_t recv(int fd, void *buf, size_t n, int flags);
int close(int fd);
int inet_pton(int af, const char *src, void *dst);
int select(int nfds, struct fd_set *readfds, struct fd_set *writefds, struct fd_set *exceptfds, struct timeval *timeout);
int gettimeofday(struct timeval *tp, void *tzp);
int usleep(unsigned int usec);
int setenv(const char *name, const char *value, int overwrite);
int signal(int signum, void (*handler)(int));

FILE *fopen(const char *path, const char *mode);
size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream);
int fclose(FILE *stream);
]]

-- SIGPIPE = 13, SIG_IGN = 1: a write to a closed socket must not kill the VM.
C.signal(13, ffi.cast("void(*)(int)", 1))

-- macOS BSD sockaddr_in starts with sin_len; Linux does not. Both keep
-- sin_port at offset 2 and sin_addr at offset 4, so this is the only layout
-- that differs between the two.
local IS_OSX = ffi.os == "OSX"
if not IS_OSX then
    -- redefine for Linux-like layout (sa_family_t is uint16 there)
    ffi.cdef[[
    struct sockaddr_in_linux { uint16_t sin_family; uint16_t sin_port; uint32_t sin_addr; char sin_zero[8]; };
    ]]
end

-- ---------------------------------------------------------------------------
-- Constants
-- ---------------------------------------------------------------------------

local VERSION = 1
local DEFAULT_URL = "ws://127.0.0.1:9734"
local DEFAULT_TIMEOUT_MS = 10000
local WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
local MAX_FRAME_LEN = 16 * 1024 * 1024

local AF_INET, SOCK_STREAM = 2, 1

local PROCESS_POSITION = {
    unspecified = 0,
    preprocess = 1,
    inprocess = 2,
    postprocess = 3,
    connection = 4,
}

-- proto message name -> { {field_name, field_number, field_type}, ... }
-- field_type: int32/int64/uint32/uint64/bool/enum/string/bytes/float/double/
--             msg:<Name>/rep:<inner>/map:<key>:<val>
local _MESSAGES = {
    AuthNew = {
        { "new_auth", 1, "string" },
    },
    AuthVerify = {
        { "cur_auth", 1, "string" },
    },
    Ban = {
        { "commender_uuid7", 1, "string" },
        { "commendee_uuid7", 2, "string" },
        { "unbanned", 3, "bool" },
        { "reason", 4, "string" },
        { "raw_message", 5, "string" },
        { "appeals", 6, "rep:string" },
    },
    Flag = {
        { "flag_name", 1, "string" },
        { "flag_description", 2, "string" },
        { "limiting_type", 3, "enum" },
        { "min_val", 4, "float" },
        { "max_val", 5, "float" },
        { "options", 6, "rep:string" },
    },
    Command = {
        { "command_name", 1, "string" },
        { "command_flag", 2, "string" },
        { "command_description", 3, "string" },
        { "command_flags", 4, "rep:msg:Flag" },
    },
    Commands = {
        { "commands", 1, "rep:msg:Command" },
    },
    Commendation = {
        { "commender_uuid7", 1, "string" },
        { "commendee_uuid7", 2, "string" },
        { "raw_message", 3, "string" },
        { "reason", 4, "string" },
    },
    Reprimand = {
        { "commender_uuid7", 1, "string" },
        { "commendee_uuid7", 2, "string" },
        { "raw_message", 3, "string" },
        { "reason", 4, "string" },
    },
    UserStylingTemplate = {
        { "css_properties", 1, "map:string:string" },
    },
    UserData = {
        { "uuid", 1, "string" },
        { "username", 2, "string" },
        { "is_sponsor", 3, "bool" },
        { "is_moderator", 4, "bool" },
        { "is_admin", 5, "bool" },
        { "is_owner", 6, "bool" },
        { "bans", 7, "rep:msg:Ban" },
        { "commendations", 8, "rep:msg:Commendation" },
        { "styling", 9, "msg:UserStylingTemplate" },
        { "platform_ids", 10, "map:string:string" },
    },
    ConnectionRequest = {
        { "pin", 1, "int32" },
        { "process_position", 2, "enum" },
        { "priority", 3, "uint32" },
        { "module_instance_uuid7", 4, "string" },
    },
    ConnectionRequestReturn = {
        { "new_port", 1, "uint32" },
        { "module_instance_uuid7", 2, "string" },
    },
    Err = {
        { "log", 1, "string" },
        { "blob", 2, "bytes" },
        { "trace", 3, "string" },
    },
    Log = {
        { "log", 1, "string" },
        { "blob", 2, "bytes" },
    },
    Shutdown = {
        { "reason", 1, "string" },
    },
    SendToPlatforms = {
        { "msg", 1, "string" },
        { "level", 2, "enum" },
        { "module_uuid7", 3, "string" },
        { "pid", 4, "string" },
        { "platform", 5, "string" },
        { "actor_platform", 6, "string" },
        { "actor_handle", 7, "string" },
        { "actor_uuid7", 8, "string" },
    },
    MessageAck = {
        { "message_uuid7", 1, "string" },
    },
    DatabaseQuery = {
        { "query_id", 1, "string" },
        { "sql", 2, "string" },
        { "params", 3, "rep:string" },
    },
    DatabaseQueryResult = {
        { "query_id", 1, "string" },
        { "success", 2, "bool" },
        { "error", 3, "string" },
        { "result_blob", 4, "bytes" },
    },
    ModuleControl = {
        { "action", 1, "enum" },
        { "module_name", 2, "string" },
        { "autostart", 3, "bool" },
    },
    ModuleControlResult = {
        { "success", 1, "bool" },
        { "error", 2, "string" },
        { "message", 3, "string" },
    },
    Prompt = {
        { "prompt_id_uuid7", 1, "string" },
        { "prompt", 2, "string" },
        { "details", 3, "string" },
        { "yes_dialog", 4, "string" },
        { "no_dialog", 5, "string" },
        { "timeout", 6, "uint32" },
        { "origin", 7, "string" },
        { "origin_uuid7", 8, "string" },
        { "instructions", 9, "string" },
        { "link", 10, "string" },
        { "input_label", 11, "string" },
        { "prompt_type", 12, "enum" },
    },
    PromptResponse = {
        { "prompt_id_uuid7", 1, "string" },
        { "accepted", 2, "bool" },
        { "reason", 3, "string" },
    },
    AuditFlag = {
        { "message_uuid7", 1, "string" },
        { "reason", 2, "string" },
        { "origin", 3, "string" },
    },
    ChatMessage = {
        { "platform", 1, "string" },
        { "raw_data", 2, "bytes" },
        { "raw_message", 3, "string" },
        { "user_uuid7", 4, "string" },
        { "command", 5, "msg:Command" },
        { "user_data", 6, "msg:UserData" },
    },
    MessagePreProcess = {
        { "raw_message", 1, "msg:ChatMessage" },
        { "message_uuid7", 2, "string" },
        { "audio", 3, "bytes" },
        { "audio_type", 4, "string" },
    },
    MessageInProcess = {
        { "raw_message", 1, "msg:ChatMessage" },
        { "processed_message", 2, "string" },
        { "abandon_message", 3, "bool" },
        { "message_uuid7", 4, "string" },
        { "audio", 5, "bytes" },
        { "audio_type", 6, "string" },
    },
    MessagePostProcess = {
        { "raw_message", 1, "msg:ChatMessage" },
        { "processed_message", 2, "string" },
        { "message_uuid7", 3, "string" },
        { "audio", 4, "bytes" },
        { "audio_type", 5, "string" },
    },
    TimelineEvent = {
        { "timeline_id_uuid7", 1, "string" },
        { "event_type", 2, "enum" },
        { "command_flag", 3, "string" },
        { "data_blob", 4, "bytes" },
        { "error_message", 5, "string" },
        { "raw_flags", 6, "string" },
        { "message_origin", 7, "string" },
        { "stream_origin", 8, "string" },
        { "raw_message", 9, "string" },
        { "processed_message", 10, "string" },
        { "user_uuid7", 11, "string" },
        { "version", 12, "uint32" },
    },
}

-- Container.payload oneof: {client_name, field_number, proto_message_name}
local _PAYLOAD = {
    { "connectionRequest", 7, "ConnectionRequest" },
    { "connectionRequestReturn", 8, "ConnectionRequestReturn" },
    { "authVerify", 9, "AuthVerify" },
    { "authNew", 10, "AuthNew" },
    { "commandPayload", 11, "Command" },
    { "commandsPayload", 12, "Commands" },
    { "messagePreProcess", 13, "MessagePreProcess" },
    { "messageInProcess", 14, "MessageInProcess" },
    { "messagePostProcess", 15, "MessagePostProcess" },
    { "timelineEvent", 16, "TimelineEvent" },
    { "userData", 17, "UserData" },
    { "shutdown", 18, "Shutdown" },
    { "log", 19, "Log" },
    { "err", 20, "Err" },
    { "sendToPlatforms", 21, "SendToPlatforms" },
    { "messageAck", 22, "MessageAck" },
    { "databaseQuery", 23, "DatabaseQuery" },
    { "databaseQueryResult", 24, "DatabaseQueryResult" },
    { "moduleControl", 25, "ModuleControl" },
    { "moduleControlResult", 26, "ModuleControlResult" },
    { "prompt", 27, "Prompt" },
    { "promptResponse", 28, "PromptResponse" },
    { "auditFlag", 29, "AuditFlag" },
}

local _PAYLOAD_FIELDS = {}
local _PAYLOAD_BY_FIELD = {}
local _FIELD_INDEX = {}
for _, p in ipairs(_PAYLOAD) do
    _PAYLOAD_FIELDS[#_PAYLOAD_FIELDS + 1] = p[1]
    _PAYLOAD_BY_FIELD[p[2]] = { p[1], p[3] }
end
for name, fields in pairs(_MESSAGES) do
    local idx = {}
    for _, f in ipairs(fields) do
        idx[f[2]] = { f[1], f[3] }
    end
    _FIELD_INDEX[name] = idx
end

-- ---------------------------------------------------------------------------
-- Byte / clock / entropy helpers
-- ---------------------------------------------------------------------------

local LE = true
do
    local probe = ffi.new("uint32_t[1]", 0x12345678)
    local p = ffi.cast("uint8_t*", probe)
    LE = p[0] == 0x78
end

local function now_ms()
    local tv = ffi.new("struct timeval")
    C.gettimeofday(tv, nil)
    return tonumber(tv.tv_sec) * 1000 + math.floor(tonumber(tv.tv_usec) / 1000)
end

local function sleep_ms(ms)
    C.usleep(math.floor(ms * 1000))
end

local function random_bytes(n)
    local f = C.fopen("/dev/urandom", "rb")
    if f ~= nil then
        local buf = ffi.new("uint8_t[?]", n)
        C.fread(buf, 1, n, f)
        C.fclose(f)
        return ffi.string(buf, n)
    end
    local out = {}
    for i = 1, n do
        out[i] = string.char(math.random(0, 255))
    end
    return table.concat(out)
end

local B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
local function base64_encode(s)
    local out = {}
    local n = #s
    local i = 1
    while i <= n do
        local a = string.byte(s, i) or 0
        local b = string.byte(s, i + 1) or 0
        local c = string.byte(s, i + 2) or 0
        local pad = i + 2 <= n and 0 or (i + 1 <= n and 1 or 2)
        out[#out + 1] = B64:sub(bit.rshift(a, 2) + 1, bit.rshift(a, 2) + 1)
        out[#out + 1] = B64:sub(bit.bor(bit.lshift(bit.band(a, 0x3), 4), bit.rshift(b, 4)) + 1, bit.bor(bit.lshift(bit.band(a, 0x3), 4), bit.rshift(b, 4)) + 1)
        if pad == 2 then
            out[#out + 1] = "="
        else
            out[#out + 1] = B64:sub(bit.bor(bit.lshift(bit.band(b, 0xF), 2), bit.rshift(c, 6)) + 1, bit.bor(bit.lshift(bit.band(b, 0xF), 2), bit.rshift(c, 6)) + 1)
        end
        if pad >= 1 then
            out[#out + 1] = "="
        else
            out[#out + 1] = B64:sub(bit.band(c, 0x3F) + 1, bit.band(c, 0x3F) + 1)
        end
        i = i + 3
    end
    return table.concat(out)
end

-- Pure-Lua SHA-1 (FIPS 180-1), inputs kept well below 2^29 bytes.
local function sha1(msg)
    local function rol(x, n)
        return bit.bor(bit.lshift(x, n), bit.rshift(x, 32 - n))
    end

    local h0, h1, h2, h3, h4 = 0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0
    local ml = #msg * 8

    local pad = "\128" .. string.rep("\0", (56 - (#msg + 1) % 64) % 64)
    local lenbytes = {}
    -- 64-bit big-endian bit length. NOTE: bit.rshift masks shift amounts mod
    -- 32, so shifts >= 32 are computed with math.floor (exact for ml < 2^53).
    for i = 7, 0, -1 do
        local shift = i * 8
        lenbytes[8 - i] = string.char(math.floor(ml / 2 ^ shift) % 256)
    end
    local padded = msg .. pad .. table.concat(lenbytes)

    local blocks = #padded / 64
    for block = 0, blocks - 1 do
        local w = {}
        for i = 0, 15 do
            local j = block * 64 + i * 4 + 1
            w[i] = (string.byte(padded, j) * 0x1000000)
                 + (string.byte(padded, j + 1) * 0x10000)
                 + (string.byte(padded, j + 2) * 0x100)
                 + string.byte(padded, j + 3)
        end
        for i = 16, 79 do
            local x = bit.bxor(bit.bxor(bit.bxor(w[i - 3], w[i - 8]), w[i - 14]), w[i - 16])
            w[i] = rol(x, 1)
        end
        local a, b, c, d, e = h0, h1, h2, h3, h4
        for i = 0, 79 do
            local f, k
            if i < 20 then
                f = bit.bor(bit.band(b, c), bit.band(bit.bnot(b), d))
                k = 0x5A827999
            elseif i < 40 then
                f = bit.bxor(bit.bxor(b, c), d)
                k = 0x6ED9EBA1
            elseif i < 60 then
                f = bit.bor(bit.bor(bit.band(b, c), bit.band(b, d)), bit.band(c, d))
                k = 0x8F1BBCDC
            else
                f = bit.bxor(bit.bxor(b, c), d)
                k = 0xCA62C1D6
            end
            local t = rol(a, 5) + f + e + k + w[i]
            e = d
            d = c
            c = rol(b, 30)
            b = a
            a = bit.band(t, 0xFFFFFFFF)
        end
        h0 = bit.band(h0 + a, 0xFFFFFFFF)
        h1 = bit.band(h1 + b, 0xFFFFFFFF)
        h2 = bit.band(h2 + c, 0xFFFFFFFF)
        h3 = bit.band(h3 + d, 0xFFFFFFFF)
        h4 = bit.band(h4 + e, 0xFFFFFFFF)
    end

    local out = {}
    for _, h in ipairs({ h0, h1, h2, h3, h4 }) do
        out[#out + 1] = bit.tohex(h, 8)
    end
    return table.concat(out)
end

local function hex_to_bytes(hex)
    local out = {}
    for i = 1, #hex, 2 do
        out[#out + 1] = string.char(tonumber(hex:sub(i, i + 1), 16))
    end
    return table.concat(out)
end

local function htons(p)
    p = bit.band(p, 0xFFFF)
    return bit.bor(bit.lshift(p, 8), bit.rshift(p, 8))
end

-- ---------------------------------------------------------------------------
-- Protobuf wire primitives
-- ---------------------------------------------------------------------------

local function encode_varint(out, v)
    if type(v) == "table" then
        -- uint64 split into {hi, lo} 32-bit halves
        local hi, lo = v.hi, v.lo
        while not (hi == 0 and lo == 0) do
            local b = bit.band(lo, 0x7F)
            lo = bit.rshift(lo, 7)
            if hi ~= 0 then
                lo = bit.bor(lo, bit.lshift(bit.band(hi, 0x7F), 25))
                hi = bit.rshift(hi, 7)
            end
            if not (hi == 0 and lo == 0) then
                b = bit.bor(b, 0x80)
            end
            out[#out + 1] = string.char(b)
        end
        return
    end
    if v < 0 then
        -- sign-extended negative int32/int64: always 10 bytes
        for _ = 1, 9 do
            out[#out + 1] = "\255"
        end
        out[#out + 1] = "\1"
        return
    end
    while v >= 128 do
        out[#out + 1] = string.char(bit.bor(bit.band(v, 0x7F), 0x80))
        v = math.floor(v / 128)
    end
    out[#out + 1] = string.char(v)
end

-- Returns lo, hi (both 32-bit unsigned) of the varint at pos[1].
local function read_varint(buf, pos)
    local lo, hi = 0, 0
    for i = 0, 9 do
        local b = string.byte(buf, pos[1]) or 0
        pos[1] = pos[1] + 1
        local sh = i * 7
        if sh < 32 then
            lo = bit.bor(lo, bit.lshift(bit.band(b, 0x7F), sh))
            if sh + 7 > 32 then
                hi = bit.bor(hi, bit.rshift(bit.band(b, 0x7F), 32 - sh))
            end
        else
            hi = bit.bor(hi, bit.lshift(bit.band(b, 0x7F), sh - 32))
        end
        if bit.band(b, 0x80) == 0 then
            break
        end
    end
    return lo, hi
end

-- Combine (lo, hi) into a Lua number when safe (value < 2^53), else a
-- {hi=, lo=} table.
local function combine_u64(lo, hi)
    if hi == 0 then
        return lo
    end
    if hi < 0x200000 then
        return lo + hi * 4294967296
    end
    return { hi = hi, lo = lo }
end

local function read_varint_combined(buf, pos)
    local lo, hi = read_varint(buf, pos)
    return combine_u64(lo, hi)
end

local function to_i32(lo)
    if lo >= 0x80000000 then
        return lo - 0x100000000
    end
    return lo
end

local function encode_key(out, field_no, wire)
    encode_varint(out, bit.bor(bit.lshift(field_no, 3), wire))
end

local function encode_len_delimited(out, field_no, payload)
    encode_key(out, field_no, 2)
    encode_varint(out, #payload)
    out[#out + 1] = payload
end

local function read_bytes(buf, pos)
    local len = read_varint_combined(buf, pos)
    if pos[1] + len - 1 > #buf then
        pos[1] = #buf + 1
        return ""
    end
    local s = string.sub(buf, pos[1], pos[1] + len - 1)
    pos[1] = pos[1] + len
    return s
end

local function read_string(buf, pos)
    return read_bytes(buf, pos)
end

-- fixed32 / fixed64 via FFI (protobuf float/double are little-endian)
local function pack_float(f)
    local fp = ffi.new("float[1]", f)
    local arr = ffi.new("uint8_t[4]")
    ffi.copy(arr, fp, 4)
    local out = {}
    if LE then
        for i = 0, 3 do out[i + 1] = string.char(arr[i]) end
    else
        for i = 3, 0, -1 do out[i + 1] = string.char(arr[i]) end
    end
    return table.concat(out)
end

local function pack_double(f)
    local fp = ffi.new("double[1]", f)
    local arr = ffi.new("uint8_t[8]")
    ffi.copy(arr, fp, 8)
    local out = {}
    if LE then
        for i = 0, 7 do out[i + 1] = string.char(arr[i]) end
    else
        for i = 7, 0, -1 do out[i + 1] = string.char(arr[i]) end
    end
    return table.concat(out)
end

local function unpack_float(s)
    local arr = ffi.new("uint8_t[4]")
    for i = 0, 3 do
        arr[LE and i or (3 - i)] = string.byte(s, i + 1) or 0
    end
    local fp = ffi.new("float[1]")
    ffi.copy(fp, arr, 4)
    return fp[0]
end

local function unpack_double(s)
    local arr = ffi.new("uint8_t[8]")
    for i = 0, 7 do
        arr[LE and i or (7 - i)] = string.byte(s, i + 1) or 0
    end
    local fp = ffi.new("double[1]")
    ffi.copy(fp, arr, 8)
    return fp[0]
end

-- ---------------------------------------------------------------------------
-- Message / Container codec
-- ---------------------------------------------------------------------------

local function default_value(ftype)
    if ftype == "string" then return "" end
    if ftype == "bytes" then return "" end
    if ftype:sub(1, 4) == "msg:" then return nil end
    if ftype:sub(1, 4) == "rep:" then return {} end
    if ftype:sub(1, 4) == "map:" then return {} end
    if ftype == "bool" then return false end
    return 0
end

local function is_default_value(ftype, value)
    if value == nil then return true end
    if ftype == "string" or ftype == "bytes" then return value == "" end
    if ftype == "bool" then return not value end
    if ftype:sub(1, 4) == "msg:" then return false end
    if ftype:sub(1, 4) == "rep:" then return #value == 0 end
    if ftype:sub(1, 4) == "map:" then return next(value) == nil end
    return value == 0
end

local function encode_scalar_raw(out, ftype, item)
    if ftype == "int32" or ftype == "int64" or ftype == "uint32" or ftype == "uint64" or ftype == "enum" then
        encode_varint(out, item)
    elseif ftype == "bool" then
        encode_varint(out, item and 1 or 0)
    elseif ftype == "float" then
        out[#out + 1] = pack_float(item)
    elseif ftype == "double" then
        out[#out + 1] = pack_double(item)
    end
end

local function encode_repeated(out, field_no, inner, value)
    if type(value) ~= "table" or #value == 0 then
        return
    end
    if inner:sub(1, 4) == "msg:" then
        local msg_name = inner:sub(5)
        for _, item in ipairs(value) do
            local encoded = encode_message(msg_name, item)
            if #encoded > 0 then
                encode_len_delimited(out, field_no, encoded)
            end
        end
    elseif inner == "string" or inner == "bytes" then
        for _, item in ipairs(value) do
            encode_len_delimited(out, field_no, tostring(item))
        end
    else
        -- packed numeric repeat
        local payload = {}
        for _, item in ipairs(value) do
            encode_scalar_raw(payload, inner, item)
        end
        encode_len_delimited(out, field_no, table.concat(payload))
    end
end

local function encode_map(out, field_no, value)
    if type(value) ~= "table" then
        return
    end
    for k, v in pairs(value) do
        local entry = {}
        encode_len_delimited(entry, 1, tostring(k))
        encode_len_delimited(entry, 2, tostring(v))
        encode_len_delimited(out, field_no, table.concat(entry))
    end
end

local function encode_field(out, field_no, ftype, value)
    if value == nil then
        return
    end
    if ftype == "string" then
        if value ~= "" then
            encode_len_delimited(out, field_no, tostring(value))
        end
    elseif ftype == "bytes" then
        if value ~= "" then
            encode_len_delimited(out, field_no, value)
        end
    elseif ftype == "int32" or ftype == "int64" or ftype == "uint32" or ftype == "uint64" or ftype == "enum" then
        if value ~= 0 then
            encode_key(out, field_no, 0)
            encode_varint(out, value)
        end
    elseif ftype == "bool" then
        if value then
            encode_key(out, field_no, 0)
            encode_varint(out, 1)
        end
    elseif ftype == "float" then
        if value ~= 0 then
            encode_key(out, field_no, 5)
            out[#out + 1] = pack_float(value)
        end
    elseif ftype == "double" then
        if value ~= 0 then
            encode_key(out, field_no, 1)
            out[#out + 1] = pack_double(value)
        end
    elseif ftype:sub(1, 4) == "msg:" then
        if type(value) == "table" then
            local encoded = encode_message(ftype:sub(5), value)
            if #encoded > 0 then
                encode_len_delimited(out, field_no, encoded)
            end
        end
    elseif ftype:sub(1, 4) == "rep:" then
        encode_repeated(out, field_no, ftype:sub(5), value)
    elseif ftype:sub(1, 4) == "map:" then
        encode_map(out, field_no, value)
    end
end

-- Encodes a known message from a Lua table (field name -> value) to protobuf.
function encode_message(msg_name, data)
    local out = {}
    local fields = _MESSAGES[msg_name]
    if fields and type(data) == "table" then
        for _, f in ipairs(fields) do
            encode_field(out, f[2], f[3], data[f[1]])
        end
    end
    return table.concat(out)
end

local function skip_field(wire, buf, pos)
    if wire == 0 then
        read_varint(buf, pos)
    elseif wire == 1 then
        pos[1] = math.min(pos[1] + 8, #buf)
    elseif wire == 2 then
        local len = read_varint_combined(buf, pos)
        pos[1] = math.min(pos[1] + len, #buf)
    elseif wire == 5 then
        pos[1] = math.min(pos[1] + 4, #buf)
    elseif wire == 3 then
        while pos[1] < #buf do
            local k = read_varint_combined(buf, pos)
            local w = bit.band(k, 7)
            if w == 4 then
                return
            end
            skip_field(w, buf, pos)
        end
    else
        pos[1] = #buf
    end
end

local function decode_repeated(result, name, inner, wire, buf, pos)
    if inner:sub(1, 4) == "msg:" then
        if wire == 2 then
            local blob = read_bytes(buf, pos)
            result[name][#result[name] + 1] = decode_message(inner:sub(5), blob)
            return true
        end
        return false
    end
    if inner == "string" or inner == "bytes" then
        if wire == 2 then
            result[name][#result[name] + 1] = read_bytes(buf, pos)
            return true
        end
        return false
    end
    if wire == 2 then
        local blob = read_bytes(buf, pos)
        local p = { 1 }
        while p[1] <= #blob do
            result[name][#result[name] + 1] = decode_scalar_raw(inner, blob, p)
        end
        return true
    end
    if wire == 0 or wire == 1 or wire == 5 then
        result[name][#result[name] + 1] = decode_scalar_raw(inner, buf, pos)
        return true
    end
    return false
end

local function decode_scalar_raw(inner, buf, pos)
    if inner == "int32" then
        local lo = read_varint(buf, pos)
        return to_i32(lo)
    elseif inner == "int64" or inner == "uint64" then
        return read_varint_combined(buf, pos)
    elseif inner == "uint32" then
        local lo = read_varint(buf, pos)
        return lo
    elseif inner == "bool" then
        local lo = read_varint(buf, pos)
        return lo ~= 0
    elseif inner == "enum" then
        local lo = read_varint(buf, pos)
        return lo
    elseif inner == "float" then
        local s = string.sub(buf, pos[1], pos[1] + 3)
        pos[1] = pos[1] + 4
        return unpack_float(s)
    elseif inner == "double" then
        local s = string.sub(buf, pos[1], pos[1] + 7)
        pos[1] = pos[1] + 8
        return unpack_double(s)
    end
    return 0
end

-- Decodes a known message from protobuf bytes to a Lua table. All known fields
-- are present with proto3 defaults; absent sub-messages are nil.
function decode_message(msg_name, buf)
    local result = {}
    local fields = _MESSAGES[msg_name]
    if not fields then
        return result
    end
    for _, f in ipairs(fields) do
        result[f[1]] = default_value(f[3])
    end
    local idx = _FIELD_INDEX[msg_name]
    local pos = { 1 }
    while pos[1] <= #buf do
        local start_pos = pos[1]
        local key = read_varint_combined(buf, pos)
        local field_no = bit.rshift(key, 3)
        local wire = bit.band(key, 7)
        local f = idx[field_no]
        if not f then
            skip_field(wire, buf, pos)
        else
            local name, ftype = f[1], f[2]
            local consumed = true
            if ftype == "string" then
                if wire == 2 then result[name] = read_string(buf, pos) else consumed = false end
            elseif ftype == "bytes" then
                if wire == 2 then result[name] = read_bytes(buf, pos) else consumed = false end
            elseif ftype == "int32" then
                if wire == 0 then
                    local lo = read_varint(buf, pos)
                    result[name] = to_i32(lo)
                else consumed = false end
            elseif ftype == "int64" or ftype == "uint64" then
                if wire == 0 then result[name] = read_varint_combined(buf, pos) else consumed = false end
            elseif ftype == "uint32" then
                if wire == 0 then
                    local lo = read_varint(buf, pos)
                    result[name] = lo
                else consumed = false end
            elseif ftype == "bool" then
                if wire == 0 then
                    local lo = read_varint(buf, pos)
                    result[name] = lo ~= 0
                else consumed = false end
            elseif ftype == "enum" then
                if wire == 0 then
                    local lo = read_varint(buf, pos)
                    result[name] = lo
                else consumed = false end
            elseif ftype == "float" then
                if wire == 5 then
                    result[name] = unpack_float(string.sub(buf, pos[1], pos[1] + 3))
                    pos[1] = pos[1] + 4
                else consumed = false end
            elseif ftype == "double" then
                if wire == 1 then
                    result[name] = unpack_double(string.sub(buf, pos[1], pos[1] + 7))
                    pos[1] = pos[1] + 8
                else consumed = false end
            elseif ftype:sub(1, 4) == "msg:" then
                if wire == 2 then
                    result[name] = decode_message(ftype:sub(5), read_bytes(buf, pos))
                else consumed = false end
            elseif ftype:sub(1, 4) == "rep:" then
                consumed = decode_repeated(result, name, ftype:sub(5), wire, buf, pos)
            elseif ftype:sub(1, 4) == "map:" then
                if wire == 2 then
                    local entry = {}
                    local ep = { 1 }
                    local blob = read_bytes(buf, pos)
                    while ep[1] <= #blob do
                        local ek = read_varint_combined(blob, ep)
                        local n = bit.rshift(ek, 3)
                        local w = bit.band(ek, 7)
                        if n == 1 and w == 2 then
                            entry.key = read_string(blob, ep)
                        elseif n == 2 and w == 2 then
                            entry.value = read_string(blob, ep)
                        else
                            skip_field(w, blob, ep)
                        end
                    end
                    result[name][entry.key or ""] = entry.value or ""
                else consumed = false end
            else
                consumed = false
            end
            if not consumed then
                skip_field(wire, buf, pos)
            end
        end
        if pos[1] == start_pos then
            break
        end
    end
    return result
end

-- Encodes a full Container (version/auth/module headers + at most one payload).
function encode_container(data)
    local out = {}
    if data.version and data.version ~= 0 then
        encode_key(out, 1, 0)
        encode_varint(out, data.version)
    end
    if data.auth_token and data.auth_token ~= "" then
        encode_len_delimited(out, 3, data.auth_token)
    end
    if data.module_name and data.module_name ~= "" then
        encode_len_delimited(out, 4, data.module_name)
    end
    if data.module_instance_uuid7 and data.module_instance_uuid7 ~= "" then
        encode_len_delimited(out, 5, data.module_instance_uuid7)
    end
    for _, p in ipairs(_PAYLOAD) do
        local v = data[p[1]]
        if v ~= nil then
            encode_len_delimited(out, p[2], encode_message(p[3], v))
        end
    end
    return table.concat(out)
end

-- Decodes a full Container. Payload oneof fields that are absent stay nil.
function decode_container(buf)
    local result = {
        version = 0,
        auth_token = "",
        module_name = "",
        module_instance_uuid7 = "",
    }
    for _, p in ipairs(_PAYLOAD) do
        result[p[1]] = nil
    end
    local pos = { 1 }
    while pos[1] <= #buf do
        local start_pos = pos[1]
        local key = read_varint_combined(buf, pos)
        local field_no = bit.rshift(key, 3)
        local wire = bit.band(key, 7)
        if field_no == 1 then
            if wire == 0 then
                local lo = read_varint(buf, pos)
                result.version = to_i32(lo)
            else
                skip_field(wire, buf, pos)
            end
        elseif field_no == 3 then
            if wire == 2 then result.auth_token = read_string(buf, pos) else skip_field(wire, buf, pos) end
        elseif field_no == 4 then
            if wire == 2 then result.module_name = read_string(buf, pos) else skip_field(wire, buf, pos) end
        elseif field_no == 5 then
            if wire == 2 then result.module_instance_uuid7 = read_string(buf, pos) else skip_field(wire, buf, pos) end
        else
            local p = _PAYLOAD_BY_FIELD[field_no]
            if p and wire == 2 then
                result[p[1]] = decode_message(p[2], read_bytes(buf, pos))
            else
                skip_field(wire, buf, pos)
            end
        end
        if pos[1] == start_pos then
            break
        end
    end
    return result
end

-- Returns the client name of the payload set in a decoded container ("" if none).
local function active_payload(container)
    for _, p in ipairs(_PAYLOAD) do
        if container[p[1]] ~= nil then
            return p[1]
        end
    end
    return ""
end

-- ---------------------------------------------------------------------------
-- UUID7 (RFC 9562 style: time-ordered, 32 hex chars, no dashes)
-- ---------------------------------------------------------------------------

local _uuid_counter = 0

function uuid7()
    _uuid_counter = (_uuid_counter + 1) % 16
    local ms = now_ms()
    local rb = random_bytes(8)
    local hex = string.format("%012x", ms)
    local rand_a = string.sub(string.format("%08x", string.byte(rb, 1) * 0x1000000 + string.byte(rb, 2) * 0x10000 + string.byte(rb, 3) * 0x100 + string.byte(rb, 4)), 1, 2)
    local counter_low = string.format("%01x", _uuid_counter)
    local variant = string.sub("89ab", (string.byte(rb, 5) % 4) + 1, (string.byte(rb, 5) % 4) + 1)
    local rand_b = string.format("%015x", (string.byte(rb, 6) * 0x1000000 + string.byte(rb, 7) * 0x10000 + string.byte(rb, 8) * 0x100 + string.byte(rb, 5)) % 0x1000000000000000)
    return hex .. "7" .. rand_a .. counter_low .. variant .. rand_b
end

-- ---------------------------------------------------------------------------
-- WebSocket transport (RFC6455, client frames masked) on raw FFI sockets
-- ---------------------------------------------------------------------------

local function mask_payload(payload, key)
    local k1, k2, k3, k4 = string.byte(key, 1), string.byte(key, 2), string.byte(key, 3), string.byte(key, 4)
    local out = {}
    local n = #payload
    for i = 1, n do
        local kb = k1
        local m = (i - 1) % 4
        if m == 1 then kb = k2 elseif m == 2 then kb = k3 elseif m == 3 then kb = k4 end
        out[i] = string.char(bit.bxor(string.byte(payload, i), kb))
    end
    return table.concat(out)
end

local function unmask_payload(payload, key)
    return mask_payload(payload, key)
end

-- Encode an outbound frame: FIN + opcode, masked, network byte lengths.
local function ws_encode_frame(opcode, payload)
    local n = #payload
    local hdr = { string.char(bit.bor(0x80, opcode)) }
    if n < 126 then
        hdr[2] = string.char(bit.bor(0x80, n))
    elseif n < 65536 then
        hdr[2] = string.char(bit.bor(0x80, 126))
        hdr[3] = string.char(bit.band(bit.rshift(n, 8), 0xFF))
        hdr[4] = string.char(bit.band(n, 0xFF))
    else
        hdr[2] = string.char(bit.bor(0x80, 127))
        local hi = math.floor(n / 4294967296)
        local lo = n % 4294967296
        for i = 3, 0, -1 do
            hdr[#hdr + 1] = string.char(bit.band(bit.rshift(hi, i * 8), 0xFF))
        end
        for i = 3, 0, -1 do
            hdr[#hdr + 1] = string.char(bit.band(bit.rshift(lo, i * 8), 0xFF))
        end
    end
    local key = random_bytes(4)
    return table.concat(hdr) .. key .. mask_payload(payload, key)
end

-- The CockatielClient instance.
local CockatielClient = {}
CockatielClient.__index = CockatielClient
CockatielClient.PROCESS_POSITION = PROCESS_POSITION
CockatielClient.DEFAULT_URL = DEFAULT_URL
CockatielClient.VERSION = VERSION

function CockatielClient.new(opts)
    opts = opts or {}
    local self = setmetatable({}, CockatielClient)
    self.url = opts.url or DEFAULT_URL
    self.module_name = opts.module_name or ""
    self.module_instance_uuid7 = opts.module_instance_uuid7 or ""
    self.process_position = opts.process_position or "connection"
    self.priority = opts.priority ~= nil and opts.priority or 100
    self.timeout_ms = opts.timeout_ms or DEFAULT_TIMEOUT_MS
    self.pin = opts.pin

    self.sock = nil
    self.connected = false
    self.last_error = ""
    self.auth_token = ""

    self._rxbuf = ""
    self._frag_opcode = nil
    self._frag_payload = nil
    self._stop = false

    self._handlers = {}
    self._receive_any = {}

    self:_load_local_env()
    return self
end

function CockatielClient:get_auth_token()
    return self.auth_token
end

function CockatielClient:get_module_name()
    return self.module_name
end

function CockatielClient:get_module_instance_uuid7()
    return self.module_instance_uuid7
end

function CockatielClient:get_last_error()
    return self.last_error
end

function CockatielClient:is_connected()
    return self.connected
end

-- ---------------------------------------------------------------------------
-- .env loading + PIN precedence (COCKATIEL_PIN env -> opts.pin)
-- ---------------------------------------------------------------------------

function CockatielClient:_load_local_env()
    local f = io.open(".env", "r")
    if not f then
        return
    end
    for line in f:lines() do
        line = line:gsub("^%s+", ""):gsub("%s+$", "")
        if line ~= "" and line:sub(1, 1) ~= "#" then
            local eq = line:find("=", 1, true)
            if eq then
                local k = line:sub(1, eq - 1):gsub("^%s+", ""):gsub("%s+$", "")
                local v = line:sub(eq + 1):gsub("^%s+", ""):gsub("%s+$", "")
                v = v:gsub('^"(.*)"$', "%1")
                if k ~= "" and os.getenv(k) == nil then
                    C.setenv(k, v, 0)
                end
            end
        end
    end
    f:close()
end

function CockatielClient:_resolve_pin(opts)
    local env_pin = os.getenv("COCKATIEL_PIN")
    if env_pin and env_pin ~= "" then
        local n = tonumber(env_pin)
        if n then
            return math.floor(n)
        end
    end
    local candidate = (opts and opts.pin ~= nil) and opts.pin or self.pin
    if candidate ~= nil then
        local n = tonumber(candidate)
        if n then
            return math.floor(n)
        end
        return 0
    end
    return 0
end

-- ---------------------------------------------------------------------------
-- Socket transport
-- ---------------------------------------------------------------------------

function CockatielClient:_parse_url()
    local scheme, rest = self.url:match("^(%w+)://(.+)$")
    if not scheme then
        error("invalid url: " .. self.url)
    end
    scheme = scheme:lower()
    local port
    if scheme == "ws" then
        port = 80
    elseif scheme == "wss" then
        error("wss/TLS is not supported (FFI libc has no TLS) — use ws://")
    else
        error("unsupported scheme: " .. scheme)
    end
    local host = rest
    if rest:find("/") then
        host = rest:match("^([^/]+)")
    end
    local hport = host:match(":(%d+)$")
    if hport then
        port = tonumber(hport)
        host = host:sub(1, -(#hport + 2))
    end
    if host == "localhost" then
        host = "127.0.0.1"
    end
    return host, port
end

function CockatielClient:_tcp_connect(host, port)
    local fd = C.socket(AF_INET, SOCK_STREAM, 0)
    if fd < 0 then
        error("socket() failed")
    end
    self.sock = fd
    local addr
    if IS_OSX then
        addr = ffi.new("struct sockaddr_in")
        addr.sin_len = ffi.sizeof(addr)
        addr.sin_family = AF_INET
        addr.sin_port = htons(port)
        addr.sin_addr = 0
        local ipbuf = ffi.new("uint32_t[1]")
        if C.inet_pton(AF_INET, host, ffi.cast("void*", ipbuf)) ~= 1 then
            self:_close_socket()
            error("invalid IPv4 host: " .. host)
        end
        addr.sin_addr = ipbuf[0]
        local r = C.connect(fd, ffi.cast("struct sockaddr*", addr), ffi.sizeof(addr))
        if r ~= 0 then
            self:_close_socket()
            error("connect() to " .. host .. ":" .. port .. " failed (errno " .. ffi.errno() .. ")")
        end
    else
        -- Linux layout: no sin_len, family is uint16
        local addr2 = ffi.new("struct sockaddr_in_linux")
        addr2.sin_family = AF_INET
        addr2.sin_port = htons(port)
        addr2.sin_addr = 0
        local ipbuf = ffi.new("uint32_t[1]")
        if C.inet_pton(AF_INET, host, ffi.cast("void*", ipbuf)) ~= 1 then
            self:_close_socket()
            error("invalid IPv4 host: " .. host)
        end
        addr2.sin_addr = ipbuf[0]
        local r = C.connect(fd, ffi.cast("struct sockaddr*", addr2), ffi.sizeof(addr2))
        if r ~= 0 then
            self:_close_socket()
            error("connect() to " .. host .. ":" .. port .. " failed (errno " .. ffi.errno() .. ")")
        end
    end
end

function CockatielClient:_close_socket()
    if self.sock then
        C.close(self.sock)
        self.sock = nil
    end
end

-- Returns a chunk of data, nil on timeout, "" on EOF/closed.
function CockatielClient:_recv_some(timeout_ms)
    if not self.sock then
        return ""
    end
    local fds = ffi.new("struct fd_set")
    ffi.fill(fds, ffi.sizeof(fds), 0)
    fds.fds_bits[math.floor(self.sock / 32)] = bit.lshift(1, bit.band(self.sock, 31))
    local tv
    if timeout_ms <= 0 then
        tv = ffi.new("struct timeval", 0, 0)
    else
        tv = ffi.new("struct timeval", math.floor(timeout_ms / 1000), (timeout_ms % 1000) * 1000)
    end
    local r = C.select(self.sock + 1, fds, nil, nil, tv)
    if r <= 0 then
        return nil
    end
    local buf = ffi.new("char[65536]")
    local n = C.recv(self.sock, buf, 65536, 0)
    if n < 0 then
        return nil
    end
    if n == 0 then
        return ""
    end
    return ffi.string(buf, n)
end

function CockatielClient:_send_bytes(data)
    if not self.sock then
        return false
    end
    local pos = 1
    local len = #data
    while pos <= len do
        local n = C.send(self.sock, data, len - pos + 1, 0)
        if n < 0 then
            return false
        end
        if n == 0 then
            return false
        end
        pos = pos + n
    end
    return true
end

function CockatielClient:_send_raw(opcode, payload)
    return self:_send_bytes(ws_encode_frame(opcode, payload))
end

function CockatielClient:_send_container(container)
    return self:_send_raw(0x2, encode_container(container))
end

-- ---------------------------------------------------------------------------
-- WebSocket handshake (HTTP/1.1 upgrade)
-- ---------------------------------------------------------------------------

function CockatielClient:_ws_handshake(host, port)
    local key = base64_encode(random_bytes(16))
    local request = "GET / HTTP/1.1\r\n"
        .. "Host: " .. host .. ":" .. tostring(port) .. "\r\n"
        .. "Upgrade: websocket\r\n"
        .. "Connection: Upgrade\r\n"
        .. "Sec-WebSocket-Key: " .. key .. "\r\n"
        .. "Sec-WebSocket-Version: 13\r\n\r\n"
    if not self:_send_bytes(request) then
        error("failed to send HTTP upgrade request")
    end

    local deadline = now_ms() + self.timeout_ms
    while true do
        local idx = self._rxbuf:find("\r\n\r\n", 1, true)
        if idx then
            local head = self._rxbuf:sub(1, idx + 3)
            self._rxbuf = self._rxbuf:sub(idx + 4)
            local status = head:match("HTTP/1%.1 (%d+)")
            if status ~= "101" then
                error("websocket handshake rejected: HTTP " .. tostring(status))
            end
            local accept
            for line in head:gmatch("[^\r\n]+") do
                local k, v = line:match("^([^:]+):%s*(.*)$")
                if k and k:lower() == "sec-websocket-accept" then
                    accept = v:match("^%s*(%S+)%s*$")
                    break
                end
            end
            local expected = base64_encode(hex_to_bytes(sha1(key .. WS_GUID)))
            if accept and accept ~= expected then
                error("websocket handshake failed: Sec-WebSocket-Accept mismatch")
            end
            return true
        end
        if now_ms() >= deadline then
            error("websocket handshake timed out")
        end
        local chunk = self:_recv_some(500)
        if chunk == "" then
            error("connection closed during websocket handshake")
        end
        if chunk then
            self._rxbuf = self._rxbuf .. chunk
        end
    end
end

-- ---------------------------------------------------------------------------
-- Frame parsing
-- ---------------------------------------------------------------------------

-- Pull one complete frame off _rxbuf; returns {opcode=, payload=, fin=} or nil.
function CockatielClient:_try_read_frame()
    local buf = self._rxbuf
    if #buf < 2 then
        return nil
    end
    local b0 = string.byte(buf, 1)
    local b1 = string.byte(buf, 2)
    local fin = bit.band(b0, 0x80) ~= 0
    local opcode = bit.band(b0, 0x0F)
    local masked = bit.band(b1, 0x80) ~= 0
    local len = bit.band(b1, 0x7F)
    local pos = 3
    if len == 126 then
        if #buf < pos + 2 then return nil end
        len = bit.bor(bit.lshift(string.byte(buf, pos), 8), string.byte(buf, pos + 1))
        pos = pos + 2
    elseif len == 127 then
        if #buf < pos + 8 then return nil end
        local hi, lo = 0, 0
        for i = 0, 3 do hi = hi * 256 + string.byte(buf, pos + i) end
        for i = 4, 7 do lo = lo * 256 + string.byte(buf, pos + i) end
        pos = pos + 8
        if hi > 0 then
            len = MAX_FRAME_LEN + 1
        else
            len = lo
        end
    end
    if len > MAX_FRAME_LEN then
        self._rxbuf = ""
        error("websocket frame too large")
    end
    local mask_key
    if masked then
        if #buf < pos + 4 then return nil end
        mask_key = string.sub(buf, pos, pos + 3)
        pos = pos + 4
    end
    if #buf < pos + len then
        return nil
    end
    local payload = string.sub(buf, pos, pos + len - 1)
    self._rxbuf = string.sub(buf, pos + len)
    if masked then
        payload = unmask_payload(payload, mask_key)
    end
    return { opcode = opcode, payload = payload, fin = fin }
end

function CockatielClient:_handle_raw_frame(frame)
    local opcode = frame.opcode
    if opcode == 0x8 then
        -- close: reply with close, drop the socket
        self:_send_raw(0x8, frame.payload)
        self.connected = false
        return
    elseif opcode == 0x9 then
        self:_send_raw(0xA, frame.payload)
        return
    elseif opcode == 0xA then
        return
    elseif opcode == 0x0 then
        if self._frag_opcode then
            self._frag_payload = self._frag_payload .. frame.payload
            if frame.fin then
                local o, p = self._frag_opcode, self._frag_payload
                self._frag_opcode, self._frag_payload = nil, nil
                self:_handle_complete_frame(o, p)
            end
        end
        return
    elseif opcode == 0x1 or opcode == 0x2 then
        if frame.fin then
            self:_handle_complete_frame(opcode, frame.payload)
        else
            self._frag_opcode = opcode
            self._frag_payload = frame.payload
        end
        return
    end
end

function CockatielClient:_handle_complete_frame(opcode, payload)
    if opcode ~= 0x2 then
        return
    end
    local ok, container = pcall(decode_container, payload)
    if not ok then
        return
    end
    self:_dispatch(container)
end

-- Auto-answer AuthVerify and dispatch to handlers (inside the receive path).
function CockatielClient:_dispatch(container)
    local active = active_payload(container)
    if active == "authVerify" then
        if self.auth_token ~= "" then
            local reply = {
                version = VERSION,
                auth_token = self.auth_token,
                module_name = self.module_name,
                module_instance_uuid7 = self.module_instance_uuid7,
                authVerify = { cur_auth = self.auth_token },
            }
            self:_send_container(reply)
        end
        return
    end
    for _, cb in ipairs(self._receive_any) do
        cb(container, active)
    end
    if active ~= "" and self._handlers[active] then
        for _, cb in ipairs(self._handlers[active]) do
            cb(container[active])
        end
    end
end

-- ---------------------------------------------------------------------------
-- Public API
-- ---------------------------------------------------------------------------

--- Registers a typed handler for a payload field name. Chainable.
function CockatielClient:on(field_name, cb)
    if not self._handlers[field_name] then
        self._handlers[field_name] = {}
    end
    self._handlers[field_name][#self._handlers[field_name] + 1] = cb
    return self
end

--- Registers an all-catch listener: called with (container, active_field).
function CockatielClient:receive_any(cb)
    self._receive_any[#self._receive_any + 1] = cb
    return self
end

--- Connects to the engine: single-connection PIN -> JWT auth on one socket.
function CockatielClient:connect(opts)
    opts = opts or {}
    if self.module_name == "" or self.module_name == "unnamed_module" then
        error("module_name must be set (engine rejects blank/unnamed identities)")
    end
    local host, port = self:_parse_url()
    local pin = self:_resolve_pin(opts)
    local pp = self.process_position
    if type(pp) == "string" then
        pp = PROCESS_POSITION[pp:lower()] or PROCESS_POSITION.connection
    end

    self:_tcp_connect(host, port)
    self._rxbuf = ""
    self:_ws_handshake(host, port)

    local req = {
        version = VERSION,
        auth_token = "",
        module_name = self.module_name,
        module_instance_uuid7 = "",
        connectionRequest = {
            pin = pin,
            process_position = pp,
            priority = self.priority,
            module_instance_uuid7 = "",
        },
    }
    if not self:_send_container(req) then
        self:_close_socket()
        error("failed to send ConnectionRequest")
    end

    local container = self:_read_container(self.timeout_ms)
    if not container then
        self:_close_socket()
        error("timed out waiting for ConnectionRequestReturn from " .. self.url)
    end
    local ret = container.connectionRequestReturn
    if not ret then
        self:_close_socket()
        error("engine did not reply with a connectionRequestReturn")
    end
    if ret.new_port and ret.new_port ~= 0 then
        self:_close_socket()
        error("engine requested an unsupported port hop (new_port=" .. tostring(ret.new_port) .. ")")
    end
    local token = container.auth_token or ""
    if token == "" then
        self:_close_socket()
        error("engine rejected the connection request (no auth token)")
    end
    self.auth_token = token
    local inst = ret.module_instance_uuid7 or ""
    if inst == "" then
        inst = container.module_instance_uuid7 or ""
    end
    self.module_instance_uuid7 = inst
    self.connected = true
    self._stop = false
    return true
end

--- Wraps a payload in a Container and sends it. field_name must be one of the
--- 23 client payload names (e.g. "log", "messageAck", "promptResponse").
function CockatielClient:send(field_name, payload)
    if not self.connected or not self.sock then
        self.last_error = "not connected to engine"
        return false
    end
    local known = false
    for _, f in ipairs(_PAYLOAD_FIELDS) do
        if f == field_name then known = true break end
    end
    if not known then
        self.last_error = "unknown payload field: " .. tostring(field_name)
        return false
    end
    local container = {
        version = VERSION,
        auth_token = self.auth_token,
        module_name = self.module_name,
        module_instance_uuid7 = self.module_instance_uuid7,
    }
    container[field_name] = payload
    local ok = self:_send_container(container)
    if not ok then
        self.last_error = "send failed (socket write error)"
    end
    return ok
end

--- Reads a single Container from the socket (blocking), handling control
--- frames. Returns the container or nil on timeout/close.
function CockatielClient:_read_container(timeout_ms)
    local deadline = now_ms() + timeout_ms
    while now_ms() < deadline do
        local chunk = self:_recv_some(200)
        if chunk == "" then
            self.connected = false
            return nil
        end
        if chunk then
            self._rxbuf = self._rxbuf .. chunk
        end
        while true do
            local ok, frame = pcall(function() return self:_try_read_frame() end)
            if not ok or not frame then
                break
            end
            if frame.opcode == 0x9 then
                self:_send_raw(0xA, frame.payload)
            elseif frame.opcode == 0x8 then
                self.connected = false
                return nil
            elseif frame.opcode == 0x2 then
                local cok, c = pcall(decode_container, frame.payload)
                if cok then
                    return c
                end
            end
        end
    end
    return nil
end

--- Non-blocking receive: drains any available frames, auto-answers AuthVerify
--- and dispatches to handlers. Returns the number of containers processed.
function CockatielClient:poll()
    if not self.connected or not self.sock then
        return 0
    end
    local count = 0
    while self.connected and self.sock do
        local frame = self:_try_read_frame()
        if frame then
            local ok, err = pcall(function() self:_handle_raw_frame(frame) end)
            if not ok then
                self.last_error = tostring(err)
            end
            count = count + 1
        else
            local chunk = self:_recv_some(0)
            if chunk == "" then
                self.connected = false
                break
            end
            if not chunk then
                break
            end
            self._rxbuf = self._rxbuf .. chunk
        end
    end
    return count
end

--- Blocking receive loop. Runs until disconnect() is called or the socket
--- closes. The optional callback is invoked with (container, active_field) for
--- every non-AuthVerify container, mirroring receive_any.
function CockatielClient:receive_loop(callback)
    if callback then
        self:receive_any(callback)
    end
    self._stop = false
    while self.connected and not self._stop and self.sock do
        local chunk = self:_recv_some(500)
        if chunk == "" then
            self.connected = false
            break
        end
        if chunk then
            self._rxbuf = self._rxbuf .. chunk
        end
        self:poll()
    end
end

--- Reconnects with the stored JWT (no PIN needed). The engine gates the first
--- frame on any fresh socket to a ConnectionRequest, so the reauth container
--- carries one alongside the token; the engine ignores its contents.
function CockatielClient:reconnect()
    if self.auth_token == "" then
        error("cannot reconnect without an auth token")
    end
    local host, port = self:_parse_url()
    self:_tcp_connect(host, port)
    self._rxbuf = ""
    self:_ws_handshake(host, port)
    local pp = self.process_position
    if type(pp) == "string" then
        pp = PROCESS_POSITION[pp:lower()] or PROCESS_POSITION.connection
    end
    local reauth = {
        version = VERSION,
        auth_token = self.auth_token,
        module_name = self.module_name,
        module_instance_uuid7 = self.module_instance_uuid7,
        connectionRequest = {
            pin = 0,
            process_position = pp,
            priority = self.priority,
            module_instance_uuid7 = self.module_instance_uuid7,
        },
    }
    if not self:_send_container(reauth) then
        self:_close_socket()
        self.connected = false
        return false
    end
    self.connected = true
    self._stop = false
    return true
end

--- Closes the connection: best-effort shutdown payload, WS close frame, socket.
function CockatielClient:disconnect()
    self._stop = true
    if self.connected and self.sock then
        pcall(function() self:send("shutdown", { reason = "" }) end)
        self:_send_raw(0x8, "")
    end
    self:_close_socket()
    self.connected = false
end

function CockatielClient:close()
    self:disconnect()
end

-- Expose the codec statically for reuse by tests / other modules.
CockatielClient.encode_container = encode_container
CockatielClient.decode_container = decode_container
CockatielClient.encode_message = encode_message
CockatielClient.decode_message = decode_message
CockatielClient.active_payload = active_payload
CockatielClient.uuid7 = uuid7
CockatielClient.sleep_ms = sleep_ms
CockatielClient.now_ms = now_ms
CockatielClient._PAYLOAD_FIELDS = _PAYLOAD_FIELDS
CockatielClient._MESSAGES = _MESSAGES
CockatielClient._PAYLOAD = _PAYLOAD

-- ---------------------------------------------------------------------------
-- Codec / wire self-test (no server needed). Returns {ok, failures}.
-- ---------------------------------------------------------------------------

function CockatielClient.codec_self_test()
    local failures = {}

    local function expect(cond, msg)
        if not cond then
            failures[#failures + 1] = msg
        end
    end

    -- 1. Known byte sequence: Log { log: "hi" } -> 0x0A 0x02 0x68 0x69
    local log_enc = encode_message("Log", { log = "hi" })
    expect(log_enc == "\10\2hi", "Log bytes mismatch: " .. string.format("%02x", string.byte(log_enc, 1)) .. " ...")

    -- 2. ConnectionRequest { pin: 150, priority: 100 }
    -- pin field1 varint(150): 0x08 0x96 0x01 ; priority field3: 0x18 0x64
    local cr = encode_message("ConnectionRequest", { pin = 150, priority = 100 })
    expect(cr == "\8\150\1\24\100", "ConnectionRequest bytes mismatch")

    -- 3. Container round trip with nested connectionRequest
    local container = {
        version = 1,
        auth_token = "",
        module_name = "lua-check",
        module_instance_uuid7 = "12345678901234567890123456789012",
        connectionRequest = { pin = 849820, process_position = 4, priority = 100, module_instance_uuid7 = "" },
    }
    local ct = decode_container(encode_container(container))
    expect(ct.version == 1 and ct.module_name == "lua-check", "Container header round trip failed")
    expect(active_payload(ct) == "connectionRequest", "active_payload mismatch")
    local cr2 = ct.connectionRequest
    expect(cr2 and cr2.pin == 849820 and cr2.priority == 100 and cr2.process_position == 4,
        "connectionRequest round trip failed")

    -- 4. Rich nested round trip: UserData (bools, repeated msgs, maps)
    local ud = {
        uuid = "abc",
        username = "lua-user",
        is_sponsor = true,
        is_moderator = false,
        bans = {
            { commender_uuid7 = "c1", commendee_uuid7 = "c2", unbanned = true, appeals = { "a", "b" } },
            { reason = "spam", appeals = {} },
        },
        styling = { css_properties = { color = "#fff", rank = "gold" } },
        platform_ids = { twitch = "handle1" },
    }
    local ud_dec = decode_message("UserData", encode_message("UserData", ud))
    expect(ud_dec.username == "lua-user" and ud_dec.is_sponsor == true and ud_dec.is_moderator == false,
        "UserData scalars round trip failed")
    expect(#ud_dec.bans == 2, "UserData bans size mismatch")
    expect(ud_dec.bans[1].unbanned == true and #ud_dec.bans[1].appeals == 2, "UserData ban round trip failed")
    expect(ud_dec.styling.css_properties.color == "#fff", "UserData styling map failed")
    expect(ud_dec.platform_ids.twitch == "handle1", "UserData platform_ids map failed")

    -- 5. Flag floats + repeated options
    local flag = { flag_name = "pf", min_val = 1.5, max_val = 9.75, options = { "x", "y" }, limiting_type = 3 }
    local fl_dec = decode_message("Flag", encode_message("Flag", flag))
    expect(math.abs(fl_dec.min_val - 1.5) < 0.0001 and math.abs(fl_dec.max_val - 9.75) < 0.0001,
        "Flag floats failed")
    expect(#fl_dec.options == 2 and fl_dec.limiting_type == 3, "Flag options/enum failed")

    -- 6. Err bytes round trip
    local err_obj = { log = "boom", blob = "\1\2\3\255", trace = "t" }
    local err_dec = decode_message("Err", encode_message("Err", err_obj))
    expect(err_dec.blob == "\1\2\3\255" and err_dec.log == "boom", "Err bytes round trip failed")

    -- 7. SHA-1 vector
    expect(sha1("abc") == "a9993e364706816aba3e25717850c26c9cd0d89d", "SHA1('abc') vector failed")

    -- 8. RFC6455 handshake accept sample:
    --    key "dGhlIHNhbXBsZSBub25jZQ==" -> accept "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    local accept = base64_encode(hex_to_bytes(sha1("dGhlIHNhbXBsZSBub25jZQ==" .. WS_GUID)))
    expect(accept == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", "RFC6455 accept sample failed: " .. accept)

    -- 9. WS frame encode (masked) round trip
    local frame_bytes = ws_encode_frame(0x2, "hello")
    -- manually decode: header, mask, unmasked payload must equal "hello"
    local b1 = string.byte(frame_bytes, 2)
    expect(bit.band(b1, 0x80) ~= 0, "client frame not masked")
    local len = bit.band(b1, 0x7F)
    expect(len == 5, "frame length mismatch")
    local key = string.sub(frame_bytes, 3, 6)
    local masked = string.sub(frame_bytes, 7, 7 + 4)
    expect(unmask_payload(masked, key) == "hello", "frame masking round trip failed")

    -- 10. uuid7 shape
    local u = uuid7()
    expect(#u == 32 and u:sub(13, 13) == "7", "uuid7 shape failed: " .. u)

    return { ok = #failures == 0, failures = failures }
end

return CockatielClient