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
| C | `#include <cockatiel_lib.h>` + 1 cmake link line | [`c/cockatiel_lib.h`](c/cockatiel_lib.h) | nanopb (vendored), libwebsockets |
| C++ | `#include <cockatiel_lib.hpp>` | [`cpp11/cockatiel_lib.hpp`](cpp11/cockatiel_lib.hpp) | thin RAII wrapper over the C lib |
| gdScript | `preload("res://cockatiel_lib.gd")` | [`gdscript/cockatiel_lib.gd`](gdscript/cockatiel_lib.gd) | Godot built-in `WebSocketPeer` |
| Odin | `import ck "cockatiel_lib"` | [`odin/cockatiel_lib/cockatiel_lib.odin`](odin/cockatiel_lib/cockatiel_lib.odin) | Odin stdlib (`core:net`) |
| Java | `import cockatiel.Cockatiel;` | [`java/Cockatiel.java`](java/Cockatiel.java) | vendored `protobuf-java` jar, `java.net.http.WebSocket` |
| Lua | `local lib = require("cockatiel_lib")` | [`lua/cockatiel_lib.lua`](lua/cockatiel_lib.lua) | LuaJIT FFI only (zero deps) |

Python and Rust live in their standalone SDK repos (git-rev pinned by consumers,
so the wire format a module builds against never shifts); everything else lives
here. All repos share this same vendored proto.

## Layout

Each language lives in its own **self-contained folder**: copy that one folder
(plus its documented dependencies) and import — nothing is reached outside it at
runtime. The root `cockatiel_protobuf.proto` is the canonical reference; folders
that need the proto at runtime vendor their own copy.

```
cockatiel_protobuf.proto   canonical wire format (pinned by SHA)
CLIENT_CONTRACT.md         the spec every language client implements
javascript/                JS client (.mjs + vendored proto + package.json)
c/                         C client (cmake; nanopb runtime vendored)
dotnet/                    C# client (single .cs, generated proto inline)
gdscript/                  gdScript client (single .gd, codec inline)
cpp11/                     C++ wrapper over the C client (link ../c)
odin/                      Odin client (native, codec inline)
java/                      Java client (single .java + vendored jar)
lua/                       Lua client (single .lua, LuaJIT FFI)
```