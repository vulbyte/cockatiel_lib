# Cockatiel Java client

A self-contained, single-source-file Java client for the Cockatiel chat engine
(protocol in [`../cockatiel_protobuf.proto`](../cockatiel_protobuf.proto),
wire behavior in [`../CLIENT_CONTRACT.md`](../CLIENT_CONTRACT.md)). One
WebSocket carries the whole session: a `ConnectionRequest` (PIN) is answered by
a `ConnectionRequestReturn` carrying a JWT, and every later frame reuses that
JWT on the same socket.

## Layout

- **`Cockatiel.java`** — the whole library in one file, package `cockatiel`.
  - `Cockatiel` — the public holder for the generated protobuf classes (all 23
    `Container` payload types + enums), e.g. `Cockatiel.Container`,
    `Cockatiel.MessagePreProcess`, `Cockatiel.DatabaseQuery`.
  - `Cockatiel.CockatielClient` — the client.
  - `Cockatiel.CockatielClient.CockatielClientOptions` — connection settings.
  - `Cockatiel.CockatielClient.Uuid7` — RFC 9562 UUIDv7 generator.
- **`codegen/generate-cockatiel-java.sh`** — regenerates `Cockatiel.java` from
  the vendored proto (`protoc --java_out`, then a package/holder remap and the
  handwritten client splice). Edit `codegen/CockatielClient.java` for client
  tweaks; re-run only after the `.proto` changes.
- **`lib/protobuf-java-4.35.1.jar`** — the vendored runtime jar (Maven Central).
- **`chain_test/ChainTest.java`** — live end-to-end chain test.

## Requirements

- OpenJDK 11+ (uses the built-in `java.net.http.WebSocket`).
- The vendored jar on the classpath (no other dependencies).
- `protoc` (only to regenerate; set `PROTOC` to override).

## Import

```java
import cockatiel.Cockatiel;   // the client is Cockatiel.CockatielClient
```

Java allows only one public top-level class per `.java` file, so the generated
protobuf holder is the public `Cockatiel` type and everything — including the
client — hangs off it. All types are public.

## Build & use

```bash
# put the vendored jar on the classpath and compile
javac -cp lib/protobuf-java-4.35.1.jar Cockatiel.java YourModule.java
java -cp .:lib/protobuf-java-4.35.1.jar YourModule
```

```java
import cockatiel.Cockatiel;

Cockatiel.CockatielClient.CockatielClientOptions opts =
        new Cockatiel.CockatielClient.CockatielClientOptions();
opts.ip = "127.0.0.1";
opts.port = 9734;
opts.pin = 123456;                 // COCKATIEL_PIN env var wins over this
opts.moduleName = "my-module";     // never blank / "unnamed_module"

// Single-connection auth: opens one socket, exchanges the PIN for a JWT.
Cockatiel.CockatielClient client =
        Cockatiel.CockatielClient.connectAsync(opts).join();

// Every decoded container, plus typed handlers for specific payloads.
client.onMessage(c -> System.out.println(c.getPayloadCase()));
client.on(Cockatiel.MessagePreProcess.class, m -> {
    System.out.println("ingested: " + m.getRawMessage().getRawMessage());
});

// sendAsync maps the payload's runtime type to the Container oneof field.
client.sendAsync(Cockatiel.MessagePreProcess.newBuilder()
        .setMessageUuid7("")                     // empty -> brand-new message
        .setRawMessage(Cockatiel.ChatMessage.newBuilder()
                .setPlatform("twitch")
                .setRawMessage("hello"))
        .build());

// AuthVerify probes are answered automatically inside the receive loop.
// Slow handlers run off the receive thread, so they never delay that reply.

client.reconnect();            // fresh socket + ConnectionRequest with the JWT
client.disconnect();           // close frame, then close
```

The engine PIN is resolved `COCKATIEL_PIN` (env) → `.env` (module-local) →
`opts.pin`, never persisted anywhere. Note: Java cannot mutate the real process
environment, so `.env` values are honored as a fallback consulted after the
real env var (the engine's own contract expects real env to win; here real env
always wins and `.env` fills the gap).

## Live chain test

Launch an isolated engine, then run `ChainTest` (module `cockatiel-test-runner`
is auto-approved):

```bash
# engine (isolated workspace on port 9737, PIN 123456)
mkdir -p /tmp/ck-engine && cd /tmp/ck-engine
# config.json: { "port": 9737, ... }  and  .env: COCKATIEL_PIN=123456
/Users/insert/cockatiel/cockatiel_engine-rs/target/debug/cockatiel-engine-rs &

# compile + run the chain test
javac -cp lib/protobuf-java-4.35.1.jar -d chain_test/classes \
    Cockatiel.java chain_test/ChainTest.java
java -cp chain_test/classes:lib/protobuf-java-4.35.1.jar \
    cockatiel.ChainTest ws://127.0.0.1:9737 123456
```

The test connects as `cockatiel-test-runner`, ingests a
`MessagePreProcess` (empty `message_uuid7`, platform `test`,
`raw_message="java chain message N"`), waits ~150 ms, sends a
`DatabaseQuery` for that row, and prints `[CHAIN_OK] chain dataflow` and
exits 0 when the result has a row. Expect the engine log to show
`Auto-approving trusted module: cockatiel-test-runner`.

## Vendored runtime

- `lib/protobuf-java-4.35.1.jar`
  - `https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/4.35.1/protobuf-java-4.35.1.jar`
- Version must match the generator (`protoc 35.1` emits
  `RuntimeVersion.validateProtobufGencodeVersion(major=4, minor=35, patch=1)`);
  the older 3.x jars the C# client uses would fail this runtime check.