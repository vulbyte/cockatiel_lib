extends Node
## Headless end-to-end test: connects to the running engine as
## "cockatiel-test-runner", sends a Log + a DatabaseQuery, and quits on the
## engine's DatabaseQueryResult reply (proves connect + encode + decode + RX).

const CockatielLib := preload("res://cockatiel_lib.gd")

var _client = null
var _start_ms := 0
var _connect_ms := 0
var _quit_on := 0
var _sent_log := false
var _sent_query := false
var _sent_ingest := false
var _sent_chain_query := false
var _saw_result := false
var _chain_ok := false
var _reconnected := false
var _rx_count := 0
var _done := false
var _result_query_id := ""
var _result_text := ""
var _msg := ""


func _ready() -> void:
	_start_ms = Time.get_ticks_msec()

	# 1) Standalone codec check (works even without the engine).
	var check: Dictionary = CockatielLib.codec_self_test()
	if check["ok"]:
		print("CODEC SELF-TEST PASS")
	else:
		for f in check["failures"]:
			print("CODEC SELF-TEST FAIL: ", f)
		get_tree().quit(1)
		return

	# 2) Live engine test. URL overridable with COCKATIEL_URL (useful when the
	# engine runs on a non-default port).
	var url_env := OS.get_environment("COCKATIEL_URL")
	var url := url_env if url_env != "" else "ws://127.0.0.1:9734"
	var opts := {
		"url": url,
		"module_name": "cockatiel-test-runner",
	}
	_client = CockatielLib.new()
	_client.receive_any(Callable(self, "_on_any"))
	_client.on("databaseQueryResult", Callable(self, "_on_result"))
	_client.on("log", Callable(self, "_on_log"))

	var err: int = _client.connect_to_engine(opts)
	if err != OK:
		print("GDSOCK FAIL connect: ", _client.get_last_error())
		get_tree().quit(1)
		return
	print("GDSOCK connected module=", _client.get_module_name(),
			" instance=", _client.get_module_instance_uuid7(),
			" jwt_len=", _client.get_auth_token().length())
	_connect_ms = Time.get_ticks_msec()


func _process(_delta: float) -> void:
	if _done:
		return
	if _client != null:
		_client.poll()

	# Wait >40ms after auth: the engine drains any frame pipelined within its
	# authorization window and discards it as "sent before authorization".
	var since_connect := Time.get_ticks_msec() - _connect_ms

	if not _sent_log and since_connect > 600:
		_sent_log = true
		var err: int = _client.send_payload("log", {
			"log": "gdscript-test hello from GDScript",
			"blob": PackedByteArray(),
		})
		print("GDSOCK sent log err=", err)
	if _sent_log and not _sent_query:
		_sent_query = true
		var err: int = _client.send_payload("databaseQuery", {
			"query_id": "engine_info",
			"sql": "",
			"params": [],
		})
		print("GDSOCK sent databaseQuery err=", err)

	# Chain dataflow: ingest as an adapter (empty message_uuid7), then verify
	# the timeline row via a DatabaseQuery — mirrors the test-runner chain suite.
	if _sent_query and not _sent_ingest and since_connect > 1000:
		_sent_ingest = true
		_msg = "gdscript chain message %d" % Time.get_ticks_msec()
		var ierr: int = _client.send_payload("messagePreProcess", {
			"raw_message": {
				"platform": "test",
				"raw_message": _msg,
				"user_uuid7": "",
			},
			"message_uuid7": "",
			"audio": PackedByteArray(),
			"audio_type": "",
		})
		print("GDSOCK ingested err=", ierr, " msg=", _msg)
	if _sent_ingest and not _sent_chain_query and since_connect > 1300:
		_sent_chain_query = true
		var qerr: int = _client.send_payload("databaseQuery", {
			"query_id": "gd_chain_check",
			"sql": "SELECT pipeline_status FROM timeline_events WHERE platform = 'test' AND raw_message = '%s'" % _msg,
			"params": [],
		})
		print("GDSOCK sent chain verify query err=", qerr)

	if _chain_ok and not _reconnected:
		_reconnected = true
		var rerr: int = _client.reconnect()
		print("GDSOCK reconnect err=", rerr, " still_open=",
				_client.get_socket_state() == WebSocketPeer.STATE_OPEN)
		if rerr == OK:
			# give the engine a beat to log the reauth before we quit
			_quit_on = Time.get_ticks_msec() + 800
		else:
			_quit_on = Time.get_ticks_msec() + 50

	if _chain_ok and _reconnected and Time.get_ticks_msec() >= _quit_on:
		print("GDSOCK SUCCESS query_id=", _result_query_id,
				" result=", _result_text, " rx=", _rx_count)
		_done = true
		_client.close()
		get_tree().quit(0)
	elif Time.get_ticks_msec() - _start_ms > 20000:
		print("GDSOCK FAIL timeout rx=", _rx_count, " last_err=", _client.get_last_error())
		_done = true
		_client.close()
		get_tree().quit(1)


func _on_any(_container: Dictionary, _active: String) -> void:
	_rx_count += 1


func _on_log(lg: Dictionary) -> void:
	print("GDSOCK RX log: ", lg.get("log", ""))


func _on_result(res: Dictionary) -> void:
	_result_query_id = str(res.get("query_id", ""))
	var blob: PackedByteArray = res.get("result_blob", PackedByteArray())
	_result_text = blob.get_string_from_utf8()
	print("GDSOCK RX databaseQueryResult query_id=", _result_query_id,
			" success=", res.get("success", false), " blob=", _result_text)
	if _result_query_id == "gd_chain_check" and res.get("success", false) \
			and blob.size() > 0:
		_chain_ok = true
		print("GDSOCK CHAIN_OK")
	_saw_result = true