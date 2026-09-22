# Cockatiel C++ client (C++11)

A thin RAII wrapper over the [C client](../c/) — same wire behavior, no new
dependencies (see [`CLIENT_CONTRACT.md`](../CLIENT_CONTRACT.md)).

## One-line import

```cpp
#include <cockatiel_lib.hpp>      // RAII wrapper
#include <cockatiel_lib.h>        // C API (payload structs + enums)
```

Link the `c/` static lib + libwebsockets (the nanopb runtime is vendored inside
`c/`):

```cmake
add_subdirectory(../c c_build)
target_link_libraries(my_module PRIVATE cockatiel_lib websockets)
```

## Example

```cpp
#include <cockatiel_lib.hpp>

cockatiel::Client client;
client.connect("ws://127.0.0.1:9734", pin, "my-module",
               COCKATIEL_POSITION_POSTPROCESS, 100);

client.receive_loop([](const cockatiel::Container *c) {
    // auto-answers AuthVerify; your handler sees every other frame
});

cockatiel_protobuf_v1_Log log = cockatiel_protobuf_v1_Log_init_zero;
snprintf(log.log, sizeof(log.log), "hello");
client.send(COCKATIEL_PAYLOAD_LOG, &log);
```

## API

- `connect(url, pin, module_name, process_position, priority)` — single-connection
  auth; `pin` may be 0 when `COCKATIEL_PIN` is set.
- `send(payload_field, message)` — wraps in a Container with the current JWT.
- `receive_loop(on_container)` — blocking; answers `AuthVerify` automatically.
- `stop()` / `reconnect()` / `close()` — RAII destructor disconnects.
- `cockatiel::uuid7()` — RFC 9562 UUIDv7.

Build + run the smoke test:

```sh
c++ -std=c++11 cpp_smoke.cpp -I. -I../c -I../c/nanopb ../c/build/libcockatiel_lib.a \
   $(pkg-config --cflags --libs libwebsockets) -o cpp_smoke
COCKATIEL_PIN=<pin> ./cpp_smoke
```