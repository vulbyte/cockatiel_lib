using System;
using System.Threading;
using System.Threading.Tasks;
using Cockatiel;

// Tiny example + live smoke test: connects to the engine as the always-trusted
// module "cockatiel-test-runner", sends a Log, and prints whatever it receives.
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

        // The engine drains a ~40ms post-connect window and discards anything
        // pipelined before authorization is fully wired (see engine main.rs);
        // wait a beat so this first payload lands as a properly-authorized send.
        await Task.Delay(300);
        await client.SendAsync(new Log { Log_ = "hello from dotnet client (cockatiel-test-runner)" });

        Console.WriteLine("waiting for inbound frames...");
        await Task.Delay(3000);

        Console.WriteLine("disconnecting");
        await client.DisconnectAsync("smoke test done");
        Console.WriteLine("done");
    }
}