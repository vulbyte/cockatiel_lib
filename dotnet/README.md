# Cockatiel — C# client library

Single-file .NET client for the Cockatiel chat engine: **binary protobuf over
one WebSocket**, implementing the wire spec in [`CLIENT_CONTRACT.md`](../CLIENT_CONTRACT.md).

Everything lives in one file — `Cockatiel/Cockatiel.cs` — which contains both
the client class and the protobuf types generated from
[`cockatiel_protobuf.proto`](../cockatiel_protobuf.proto). Import it with a
single line of code.

## One-line import

```csharp
using Cockatiel;   // that's it — client + Container + payload types
```

Dependencies are minimal:

- `Google.Protobuf` (NuGet) — serialization
- `System.Net.WebSockets.ClientWebSocket` — part of the BCL

Two ways to consume it:

1. **Reference the project / built package**

   ```xml
   <PackageReference Include="Cockatiel" Version="0.1.0" />
   ```

   or `dotnet add reference ../Cockatiel/Cockatiel.csproj`.

2. **Copy the single file in** — drop `Cockatiel/Cockatiel.cs` into your
   project and add the same `Google.Protobuf` package reference. There is
   nothing else to wire up.

## Usage

```csharp
using Cockatiel;

var client = await CockatielClient.ConnectAsync(new CockatielClientOptions
{
    ModuleName = "my-module",     // engine rejects blank / unnamed_module
    Ip = "127.0.0.1",
    Port = 9734,                  // PIN: COCKATIEL_PIN env wins, else opts.Pin
});

// Listen for anything, or for a specific payload type.
client.ReceiveAny(c => Console.WriteLine($"payload={c.PayloadCase}"));
client.On<Log>(l => Console.WriteLine($"log: {l.Log_}"));

// Send any payload — the type is auto-mapped to the Container payload oneof.
await client.SendAsync(new Log { Log_ = "hello from .NET" });

// Reconnect on a fresh socket using the stored JWT (no PIN needed).
await client.ReconnectAsync();

await client.DisconnectAsync("bye");
```

The receive loop is fully autonomous: it **auto-answers `AuthVerify`** liveness
probes with your stored JWT, and dispatches user handlers on background tasks
so a slow handler never blocks the probe reply.

## What the library does

- **Single-connection auth** (§1): one WebSocket, `ConnectionRequest` with the
  PIN, engine replies `ConnectionRequestReturn` + JWT on the **same** socket.
- **PIN precedence** (§2): `COCKATIEL_PIN` env → `opts.Pin` → `0`. A
  module-local `.env` is loaded into the process environment first (real env
  wins, §2).
- **Full 23-field Container codec** (§4): every payload type from the proto,
  plus `ReceiveAny` / typed `On<T>` listeners. Malformed frames are ignored.
- **Liveness** (§5): automatic `AuthVerify` answer inside the receive loop.
- **Reconnect** (§6): fresh socket + Container carrying the stored JWT.
  Note: the engine requires a fresh connection's FIRST message to be a
  `ConnectionRequest` (main.rs:1429) — the reauth is a ConnectionRequest whose
  `auth_token` carries the stored JWT (the engine logs `Reconnected:`). The
  reference Rust/JS clients send a `Log` here and are rejected outright; this
  client sends the correct shape. Known engine gap: the reconnect branch never
  re-marks the session `authenticated` after the old socket's session is
  removed, so a reconnected session is severed on its first real payload. That
  is an engine-side limitation, not a client bug.
- **uuid7** (§9): `CockatielUuid7.NewUuid7()` for new message/instance ids
  (RFC 9562 style, time-ordered).

## Layout

```
dotnet/
├── Cockatiel/
│   ├── Cockatiel.cs          # single-file client (generated proto + handwritten client)
│   └── Cockatiel.csproj      # net8.0, Google.Protobuf only
├── codegen/
│   ├── generate-cockatiel-cs.sh    # regenerates Cockatiel.cs from the .proto
│   └── CockatielClient.partial.cs  # the handwritten client (spliced in)
├── example/
│   ├── example.csproj
│   └── Program.cs            # connect → send Log → print RX (also the smoke test)
├── Cockatiel.sln
└── README.md
```

## Building

```sh
export PATH="/usr/local/share/dotnet/x64:$PATH"
dotnet build Cockatiel.sln
```

## Regenerating `Cockatiel.cs`

Only needed if `cockatiel_protobuf.proto` changes:

```sh
cd dotnet
./codegen/generate-cockatiel-cs.sh   # requires protoc on PATH
```

The script compiles the vendored proto with `protoc --csharp_out`, remaps the
generated namespace (`CockatielProtobuf.V1` → `Cockatiel`) and splices in
`codegen/CockatielClient.partial.cs`. Edit the client in the partial and
re-run, or edit `Cockatiel.cs` directly.