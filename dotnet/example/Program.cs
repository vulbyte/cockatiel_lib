using System;
using System.Threading;
using System.Threading.Tasks;
using Cockatiel;

// Tiny example + live smoke test: connects to the engine as the always-trusted
// module "cockatiel-test-runner", sends a Log, ingests a message through the
// pipeline (adapter role: empty message_uuid7), then verifies the row landed
// in the timeline via a DatabaseQuery — the same dataflow the test-runner's
// chain suite checks.
class Program
{
    static async Task Main(string[] args)
    {
        var opts = new CockatielClientOptions
        {
            ModuleName = "cockatiel-test-runner",
            Ip = "127.0.0.1",
            Port = 9734,
            // Pin: COCKATIEL_PIN env wins, else opts.Pin (default 0).
        };

        Console.WriteLine("connecting to engine...");
        var client = await CockatielClient.ConnectAsync(opts);
        Console.WriteLine($"connected; auth token length = {client.AuthToken.Length}");
        Console.WriteLine($"module instance uuid7 = {client.ModuleInstanceUuid7}");

        client.ReceiveAny(c => Console.WriteLine($"RX container: payload={c.PayloadCase}"));
        client.On<Log>(l => Console.WriteLine($"RX Log: {l.Log_}"));

        // Watch for the chain-verification query result before sending it.
        var verified = false;
        var qid = "dotnet_chain_check";
        client.On<DatabaseQueryResult>(r =>
        {
            if (r.QueryId == qid)
            {
                var json = r.ResultBlob;
                verified = json != null && json.Length > 0 && r.Success;
                Console.WriteLine($"chain verify: success={r.Success} blobLen={json?.Length ?? 0}");
            }
        });

        // The engine drains a ~40ms post-connect window; wait a beat so the
        // first payload lands as a properly-authorized send.
        await Task.Delay(300);
        await client.SendAsync(new Log { Log_ = "hello from dotnet client (cockatiel-test-runner)" });

        // ── Chain dataflow: ingest as an adapter (empty message_uuid7) ──
        var msg = $"dotnet chain message {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        await client.SendAsync(new MessagePreProcess
        {
            MessageUuid7 = "",
            RawMessage = new ChatMessage
            {
                Platform = "test",
                RawMessage = msg,
                UserUuid7 = "",
            },
            Audio = Google.Protobuf.ByteString.Empty,
            AudioType = "",
        });
        Console.WriteLine($"ingested: {msg}");

        // Give the engine a moment to ingest, then verify the timeline row.
        await Task.Delay(150);
        await client.SendAsync(new DatabaseQuery
        {
            QueryId = qid,
            Sql = $"SELECT pipeline_status FROM timeline_events WHERE platform = 'test' AND raw_message = '{msg}'",
        });

        // Wait for the matching DatabaseQueryResult.
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(5);
        while (!verified && DateTimeOffset.UtcNow < deadline)
            await Task.Delay(50);

        Console.WriteLine(verified ? "CHAIN_OK" : "CHAIN_FAILED");
        await client.DisconnectAsync("smoke test done");
        Environment.Exit(verified ? 0 : 1);
    }
}