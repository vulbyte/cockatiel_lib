# Cockatiel Lib — one-file client imports

One import line, one client file to author against, plus declared dependencies.
Each client implements the shared contract in [`CLIENT_CONTRACT.md`](CLIENT_CONTRACT.md):
single-connection auth (PIN → JWT), `connect`/`send`/`receive`, the full
23-field `Container` payload codec, an automatic `AuthVerify` liveness answer,
and reconnect with the stored JWT.

The wire format is defined by the vendored [`cockatiel_protobuf.proto`](cockatiel_protobuf.proto),
pinned to `vulbyte/cockatiel_proto@0128454`.

## Import surface

| Language | Import line | Client file | Dependencies |
|---|---|---|---|
| JavaScript | `import { connectToEngine } from 'cockatiel-lib-js';` | [`javascript/lib-cockatiel.mjs`](javascript/lib-cockatiel.mjs) | `ws`, `protobufjs` |
| Python | `from lib_cockatiel import CockatielClient` | canonical: [`vulbyte/cockatiel_client-py`](https://github.com/vulbyte/cockatiel_client-py) | `websockets`, `grpcio-tools` |
| Rust | `use cockatiel_client::CockatielClient;` | canonical: [`vulbyte/cockatiel_client-rs`](https://github.com/vulbyte/cockatiel_client-rs) | crate (prost) |
| C# | `using Cockatiel;` + `PackageReference` | [`dotnet/Cockatiel.cs`](dotnet/Cockatiel.cs) | Google.Protobuf, `System.Net.WebSockets` |
| C | `#include <cockatiel_lib.h>` + 1 cmake link line | [`c/cockatiel_lib.h`](c/cockatiel_lib.h) | protobuf-c, a WS lib (libwebsockets) |
| C++ | `#include <cockatiel_lib.hpp>` | [`cpp11/cockatiel_lib.hpp`](cpp11/cockatiel_lib.hpp) | thin RAII wrapper over the C lib |
| gdScript | `preload("res://cockatiel_lib.gd")` | [`gdscript/cockatiel_lib.gd`](gdscript/cockatiel_lib.gd) | Godot built-in `WebSocketPeer` |

Python and Rust live in their standalone SDK repos (git-rev pinned by consumers,
so the wire format a module builds against never shifts); everything else lives
here. All repos share this same vendored proto.

## Layout

```
cockatiel_protobuf.proto   wire format (pinned by SHA)
CLIENT_CONTRACT.md         the spec every language client implements
javascript/                JS client (the only home of the .mjs lib)
c/                         C client (cmake)
dotnet/                    C# client (single .cs + package)
gdscript/                  gdScript client (single .gd)
cpp11/                     C++ wrapper over the C client
```