-- ============================================================================
-- chain_test.lua -- live chain-dataflow test for the Lua Cockatiel client.
--
-- Connects to the engine as `cockatiel-test-runner` (auto-approved), ingests
-- fresh messages via message_pre_process with EMPTY message_uuid7 (so the
-- engine assigns the row uuid and starts the pipeline), then verifies each was
-- ingested by querying the timeline for its row. Prints CHAIN_OK and exits 0
-- when the result blob has a row for every message.
--
-- NOTE: the engine build's local DB driver (turso/limbo) resolves a
-- DatabaseQuery SELECT only after a ~30s busy-timeout retry, so the per-query
-- wait below is deliberately generous (see README).
--
-- Run:   luajit chain_test.lua [ws://host:port] [pin] [iterations]
--   e.g. luajit chain_test.lua ws://127.0.0.1:9738 123456 1
-- ============================================================================

local Cockatiel = require("cockatiel_lib")

-- ---------------------------------------------------------------------------
-- Minimal JSON decoder (result_blob from the engine is a JSON array).
-- ---------------------------------------------------------------------------

local function json_decode(s)
    local pos = 1
    local function skipws()
        while pos <= #s do
            local c = s:sub(pos, pos)
            if c == " " or c == "\t" or c == "\n" or c == "\r" then
                pos = pos + 1
            else
                break
            end
        end
    end
    local function parse_value()
        skipws()
        local c = s:sub(pos, pos)
        if c == "{" then
            pos = pos + 1
            local obj = {}
            skipws()
            if s:sub(pos, pos) == "}" then
                pos = pos + 1
                return obj
            end
            while true do
                local key = parse_value()
                skipws()
                if s:sub(pos, pos) ~= ":" then
                    error("expected ':'")
                end
                pos = pos + 1
                obj[key] = parse_value()
                skipws()
                local c2 = s:sub(pos, pos)
                if c2 == "," then
                    pos = pos + 1
                elseif c2 == "}" then
                    pos = pos + 1
                    break
                else
                    error("expected ',' or '}'")
                end
            end
            return obj
        elseif c == "[" then
            pos = pos + 1
            local arr = {}
            skipws()
            if s:sub(pos, pos) == "]" then
                pos = pos + 1
                return arr
            end
            while true do
                arr[#arr + 1] = parse_value()
                skipws()
                local c2 = s:sub(pos, pos)
                if c2 == "," then
                    pos = pos + 1
                elseif c2 == "]" then
                    pos = pos + 1
                    break
                else
                    error("expected ',' or ']'")
                end
            end
            return arr
        elseif c == '"' then
            local out = {}
            pos = pos + 1
            while true do
                local ch = s:sub(pos, pos)
                if ch == '"' then
                    pos = pos + 1
                    break
                end
                if ch == "\\" then
                    local esc = s:sub(pos + 1, pos + 1)
                    if esc == "n" then
                        out[#out + 1] = "\n"
                    elseif esc == "t" then
                        out[#out + 1] = "\t"
                    elseif esc == "r" then
                        out[#out + 1] = "\r"
                    else
                        out[#out + 1] = esc
                    end
                    pos = pos + 2
                else
                    out[#out + 1] = ch
                    pos = pos + 1
                end
            end
            return table.concat(out)
        elseif c == "t" then
            pos = pos + 4
            return true
        elseif c == "f" then
            pos = pos + 5
            return false
        elseif c == "n" then
            pos = pos + 4
            return nil
        else
            local num = s:match("^-?%d+%.?%d*[eE]?[%+%-]?%d*", pos)
            if not num then
                error("bad json at " .. pos)
            end
            pos = pos + #num
            return tonumber(num)
        end
    end
    return parse_value()
end

-- ---------------------------------------------------------------------------
-- Codec self-test first (no server needed)
-- ---------------------------------------------------------------------------

local st = Cockatiel.codec_self_test()
if not st.ok then
    io.stderr:write("SELFTEST_FAIL\n")
    for _, f in ipairs(st.failures) do
        io.stderr:write("  " .. f .. "\n")
    end
    os.exit(1)
end
print("SELFTEST_OK")

-- ---------------------------------------------------------------------------
-- Config from CLI args
-- ---------------------------------------------------------------------------

local url = arg[1] or "ws://127.0.0.1:9738"
local pin = tonumber(arg[2]) or 123456
local iterations = tonumber(arg[3]) or 1
local query_deadline_ms = 45000

local client = Cockatiel.new({
    url = url,
    module_name = "cockatiel-test-runner",
    pin = pin,
    priority = 1,
    timeout_ms = 15000,
})

local ok, err = pcall(function() client:connect() end)
if not ok then
    io.stderr:write("CHAIN_FAIL connect: " .. tostring(err) .. "\n")
    os.exit(1)
end
print("CONNECTED module=" .. client:get_module_name()
    .. " instance=" .. client:get_module_instance_uuid7()
    .. " token_len=" .. #client:get_auth_token())

-- ---------------------------------------------------------------------------
-- Live chain: ingest -> wait -> query the timeline row
-- ---------------------------------------------------------------------------

local verified = 0
local failures = {}

for i = 1, iterations do
    local msg_text = "lua chain message " .. i

    -- Ingest like an adapter: EMPTY message_uuid7 -> engine assigns row uuid.
    local ok1 = client:send("messagePreProcess", {
        message_uuid7 = "",
        raw_message = {
            platform = "test",
            raw_message = msg_text,
        },
    })
    if not ok1 then
        failures[#failures + 1] = "send pre_process " .. i .. ": " .. client:get_last_error()
        break
    end

    -- Give the engine a moment to ingest, then query for the row.
    Cockatiel.sleep_ms(150)

    local qid = Cockatiel.uuid7()
    local found = false

    client:on("databaseQueryResult", function(res)
        if res.query_id == qid then
            local has_row = false
            if res.success and res.result_blob ~= "" and res.result_blob ~= "[]" then
                local parsed = json_decode(res.result_blob)
                has_row = type(parsed) == "table" and parsed[1] ~= nil
            end
            found = has_row
        end
    end)

    local ok2 = client:send("databaseQuery", {
        query_id = qid,
        sql = "SELECT pipeline_status FROM timeline_events WHERE platform = 'test' AND raw_message = '" .. msg_text .. "'",
        params = {},
    })
    if not ok2 then
        failures[#failures + 1] = "send database_query " .. i .. ": " .. client:get_last_error()
        break
    end

    -- Poll until the matching result arrives or we time out. The engine's
    -- local DB driver answers DatabaseQuery SELECTs only after a ~30s
    -- busy-timeout retry, so this can take a while.
    local deadline = Cockatiel.now_ms() + query_deadline_ms
    while not found and Cockatiel.now_ms() < deadline do
        client:poll()
        Cockatiel.sleep_ms(100)
    end

    if found then
        verified = verified + 1
    else
        failures[#failures + 1] = "msg " .. i .. " not ingested (no row)"
    end
end

client:disconnect()

if verified == iterations then
    print("CHAIN_OK (" .. verified .. "/" .. iterations .. " messages ingested + queried)")
    os.exit(0)
else
    io.stderr:write("CHAIN_FAIL verified=" .. verified .. "/" .. iterations .. "\n")
    for _, f in ipairs(failures) do
        io.stderr:write("  " .. f .. "\n")
    end
    os.exit(1)
end