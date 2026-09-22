extends Node
## Liveness probe test: connect, then stay quiet for ~14s. With a short probe
## interval the engine sends AuthVerify during dead air; the client must
## auto-answer inside its receive loop or the engine severs the session as
## "unresponsive". PASS = connection still open at the end (engine-side log must
## show no "Module unresponsive" / "Severed").

const CockatielLib := preload("res://cockatiel_lib.gd")

var _client = null
var _start_ms := 0
var _rx_count := 0
var _done := false


func _ready() -> void:
	_start_ms = Time.get_ticks_msec()
	var url_env := OS.get_environment("COCKATIEL_URL")
	var url := url_env if url_env != "" else "ws://127.0.0.1:9734"
	_client = CockatielLib.new()
	_client.receive_any(Callable(self, "_on_any"))
	var err: int = _client.connect_to_engine({"url": url, "module_name": "cockatiel-test-runner"})
	if err != OK:
		print("PROBE FAIL connect: ", _client.get_last_error())
		get_tree().quit(1)
		return
	print("PROBE connected, idling ~14s (engine probes during dead air; client must auto-answer)")


func _process(_delta: float) -> void:
	if _done:
		return
	_client.poll()
	var elapsed := Time.get_ticks_msec() - _start_ms
	if elapsed > 14000:
		var still_open: bool = _client.get_socket_state() == WebSocketPeer.STATE_OPEN
		print("PROBE idle complete: connection_still_open=", still_open, " rx=", _rx_count)
		_done = true
		_client.close()
		get_tree().quit(0 if still_open else 1)


func _on_any(_container: Dictionary, _active: String) -> void:
	_rx_count += 1