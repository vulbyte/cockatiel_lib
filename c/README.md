# C client for the Cockatiel engine

One-file import client for the Cockatiel chat engine. Implements the
[CLIENT_CONTRACT](../CLIENT_CONTRACT.md) wire spec: single-connection
PIN → JWT auth, full 23-field `Container` codec, automatic `AuthVerify`
auto-answer, reconnect with stored JWT, and PIN precedence.

## Import

```c
#include <cockatiel_lib.h>
```

Link one line (CMake, libwebsockets via pkg-config):

```cmake
find_package(PkgConfig REQUIRED)
pkg_check_modules(LWS REQUIRED IMPORTED_TARGET libwebsockets)
target_link_libraries(your_target PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cockatiel_lib PkgConfig::LWS)
```

If you vendor the source directly instead of building with the bundled
`CMakeLists.txt`, compile `cockatiel_lib.c`, `cockatiel_protobuf.pb.c` and
the nanopb runtime in `nanopb/` (`pb_common.c`, `pb_encode.c`,
`pb_decode.c`), and link `-lwebsockets`.

## Usage

```c
#include <cockatiel_lib.h>
#include <stdio.h>

static void on_container(cockatiel_client *client,
                         const cockatiel_protobuf_v1_Container *c,
                         void *userdata) {
    printf("got: %s\n", cockatiel_payload_name(c->which_payload));
    /* AuthVerify is answered automatically inside the receive loop. */
}

int main(void) {
    char errbuf[256] = {0};
    /* COCKATIEL_PIN env wins over the 0 argument (contract PIN precedence). */
    cockatiel_client *client = cockatiel_connect(
        "ws://127.0.0.1:9734", 0, "my-module",
        COCKATIEL_POSITION_POSTPROCESS, 10, errbuf, sizeof(errbuf));
    if (!client) { fprintf(stderr, "%s\n", errbuf); return 1; }

    cockatiel_protobuf_v1_Log log = cockatiel_protobuf_v1_Log_init_zero;
    snprintf(log.log, sizeof(log.log), "hello from C");
    cockatiel_send(client, COCKATIEL_PAYLOAD_LOG, &log);

    cockatiel_receive_loop(client, on_container, NULL); /* blocking */
    cockatiel_disconnect(client);
    return 0;
}
```

## API

- `cockatiel_connect(url, pin, module_name, process_position, priority, errbuf, errbuf_len)`
  Single-connection auth: opens **one** socket, sends `ConnectionRequest`,
  reads `ConnectionRequestReturn`, keeps the socket, stores the JWT. `new_port != 0`
  is treated as a protocol error.
- `cockatiel_send(client, payload_field, message)` wraps a payload struct in a
  `Container` (version=1, auth token, module identity).
- `cockatiel_receive_loop(client, on_container, userdata)` decodes every frame,
  **auto-answers `AuthVerify`** with `cur_auth = <jwt>`, and calls the callback
  for every other payload. Malformed frames are skipped. The callback must return
  quickly — the loop does not spawn threads.
- `cockatiel_reconnect(client)` fresh socket carrying the stored JWT (reauth; no PIN).
- `cockatiel_disconnect(client)`.
- `cockatiel_uuid7(out)` RFC 9562 UUIDv7 string generator.
- `cockatiel_payload_name(tag)` the 23 payload names from the contract.

The full 23-field `Container` oneof is decoded (a `ReceiveAny`-style callback gets
every container); all payload structs are static-size nanopb messages.

## Building & smoke test

```sh
cmake -S . -B build && cmake --build build
COCKATIEL_PIN=<pin> ./build/cockatiel_smoke ws://127.0.0.1:9734
```

`cockatiel_smoke` connects as `cockatiel-test-runner` (auto-approved by the
engine), sends a `Log`, and prints every inbound container.

## Layout

- `cockatiel_lib.h` / `cockatiel_lib.c` — the one-file client
- `cockatiel_protobuf.pb.h` / `.pb.c` — generated nanopb code
- `cockatiel_protobuf.options` — nanopb generator options (fixed-size buffers)
- `nanopb/` — vendored nanopb 0.4.9.1 runtime (pb.h, pb_common, pb_encode, pb_decode)
- `smoke.c` — example / smoke-test executable