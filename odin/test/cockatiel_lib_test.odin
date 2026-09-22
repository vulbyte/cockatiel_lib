package main

/*
	Cockatiel Odin client — live chain-dataflow test.

	Connects to an engine as `cockatiel-test-runner`, ingests a fresh
	MessagePreProcess (empty message_uuid7, platform="test") and queries the
	timeline for the ingested row. Prints CHAIN_OK and exits 0 on success.

	Usage:
	    odin run . [ws://127.0.0.1:9736] [pin] [module_name]
*/

import "core:fmt"
import "core:os"
import "core:strconv"
import "core:strings"
import "core:time"

import ck "../cockatiel_lib"

test_state: struct {
	result_ok:  bool,
	got_result: bool,
	chain_ok:   bool,
}

main :: proc() {
	os.exit(run())
}

run :: proc() -> int {
	args := os.args

	url := "ws://127.0.0.1:9736"
	pin: i32 = 123456
	module_name := "cockatiel-test-runner"

	if len(args) >= 2 {
		url = args[1]
	}
	if len(args) >= 3 {
		if v, ok := strconv.parse_int(args[2], 10); ok {
			pin = i32(v)
		}
	}
	if len(args) >= 4 {
		module_name = args[3]
	}

	c := ck.new_client()
	defer ck.destroy(&c)

	if !ck.connect(&c, url, pin, module_name) {
		fmt.println("connect failed:", c.last_error)
		return 1
	}
	fmt.println("connected:", c.module_name, c.module_instance_uuid7)
	fmt.println("jwt:", c.auth_token)

	// A unique per-run message so the query can never match a stale row from
	// a previous run of the same engine database.
	run_tag := ck.uuid7()
	defer delete(run_tag)
	msg := fmt.aprintf("odin chain message %s", run_tag)
	defer delete(msg)

	// 1. Ingest a brand-new message: empty message_uuid7 (adapter input).
	chat := ck.ChatMessage {
		platform    = "test",
		raw_message = msg,
	}
	pre := ck.MessagePreProcess {
		raw_message = &chat,
	}
	if !ck.send(&c, pre) {
		fmt.println("message_pre_process send failed:", c.last_error)
		return 1
	}
	fmt.println("ingested:", msg)

	// Give the engine pipeline a moment to persist the timeline event.
	time.sleep(150 * time.Millisecond)

	// 2. Query the timeline for the ingested row.
	qid := ck.uuid7()
	defer delete(qid)
	sql := fmt.aprintf(
		"SELECT pipeline_status FROM timeline_events WHERE platform = 'test' AND raw_message = '%s'",
		msg,
	)
	defer delete(sql)
	query := ck.DatabaseQuery {
		query_id = qid,
		sql      = sql,
	}

	ck.register_handler(&c, "databaseQueryResult", proc(client: ^ck.Client, container: ^ck.Container) {
		#partial switch r in container.payload {
		case ck.DatabaseQueryResult:
			test_state.got_result = true
			fmt.println("query success:", r.success, "error:", r.error)
			if len(r.result_blob) == 0 {
				fmt.println("result_blob: <empty>")
			} else {
				fmt.println("result_blob:", string(r.result_blob))
			}
			blob := string(r.result_blob)
			// A non-empty JSON result containing a row -> the chain worked.
			if r.success && strings.contains(blob, "pipeline_status") {
				test_state.chain_ok = true
				fmt.println("CHAIN_OK")
			} else {
				fmt.println("CHAIN_FAIL: no row in result blob")
			}
		}
		client.stop = true
	})

	if !ck.send(&c, query) {
		fmt.println("database_query send failed:", c.last_error)
		return 1
	}
	fmt.println("query sent:", sql)

	rc := ck.receive_loop(&c)
	fmt.println("receive_loop returned:", rc, "| got_result:", test_state.got_result)

	if test_state.chain_ok {
		return 0
	}
	return 1
}