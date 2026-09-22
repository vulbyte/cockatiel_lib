// Handwritten client. Spliced into Cockatiel.cs by codegen/generate-cockatiel-cs.sh
// (inside the namespace Cockatiel block). Edit here and re-run the script, or
// edit Cockatiel.cs directly — either stays in sync with this layout.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Reflection;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;

/// <summary>Connection settings for <see cref="CockatielClient"/>.</summary>
public sealed class CockatielClientOptions
{
    /// <summary>Engine host. Default 127.0.0.1.</summary>
    public string Ip { get; set; } = "127.0.0.1";

    /// <summary>Engine WebSocket port. Default 9734.</summary>
    public int Port { get; set; } = 9734;

    /// <summary>Engine PIN. Used only when COCKATIEL_PIN is not set; 0 = unset.</summary>
    public int Pin { get; set; }

    /// <summary>Module identity. The engine rejects blank / unnamed_module names.</summary>
    public string ModuleName { get; set; } = "";

    /// <summary>Pipeline position for the connection (default postprocess).</summary>
    public ProcessPosition ProcessPosition { get; set; } = ProcessPosition.Postprocess;

    /// <summary>Connection priority (default 100).</summary>
    public uint Priority { get; set; } = 100;

    /// <summary>Requested instance uuid7 on first connect; the engine assigns one if empty.</summary>
    public string ModuleInstanceUuid7 { get; set; } = "";
}

/// <summary>
/// Single-connection C# client for the Cockatiel chat engine.
///
/// One WebSocket carries the whole session: a ConnectionRequest (PIN) is
/// answered by a ConnectionRequestReturn carrying a JWT, and every later frame
/// reuses that JWT on the same socket. See CLIENT_CONTRACT.md.
/// </summary>
public sealed class CockatielClient : IDisposable
{
    private static readonly Dictionary<Type, PropertyInfo> _payloadSetter = BuildPayloadSetter();
    private static readonly Dictionary<Container.PayloadOneofCase, PropertyInfo> _payloadGetter = BuildPayloadGetter();

    private readonly CockatielClientOptions _opts;
    private readonly List<Action<Container>> _allHandlers = new();
    private readonly ConcurrentDictionary<Type, List<Action<object>>> _typedHandlers = new();
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    private ClientWebSocket _ws;
    private string _authToken;
    private string _instanceUuid7;
    private CancellationTokenSource? _receiveCts;
    private Task? _receiveTask;
    private volatile bool _disposed;

    private CockatielClient(CockatielClientOptions opts, ClientWebSocket ws, string authToken, string instanceUuid7)
    {
        _opts = opts;
        _ws = ws;
        _authToken = authToken;
        _instanceUuid7 = instanceUuid7;
    }

    /// <summary>The engine-assigned JWT for this session.</summary>
    public string AuthToken => _authToken;

    /// <summary>The engine-assigned (or requested) module instance uuid7.</summary>
    public string ModuleInstanceUuid7 => _instanceUuid7;

    /// <summary>The module name this client authenticates as.</summary>
    public string ModuleName => _opts.ModuleName;

    /// <summary>
    /// Open one WebSocket and authenticate with the engine (single-connection
    /// auth, contract §1). The PIN is read from COCKATIEL_PIN first, then
    /// <paramref name="opts"/>.Pin, defaulting to 0. A module-local .env file is
    /// loaded into the process environment first (real env vars win, §2).
    /// </summary>
    public static async Task<CockatielClient> ConnectAsync(CockatielClientOptions opts, CancellationToken ct = default)
    {
        if (opts is null) throw new ArgumentNullException(nameof(opts));
        if (string.IsNullOrWhiteSpace(opts.ModuleName) || opts.ModuleName == "unnamed_module")
            throw new ArgumentException("ModuleName must be a non-blank identity; the engine rejects blank/unnamed_module names.", nameof(opts));

        LoadDotEnv();

        var ws = new ClientWebSocket();
        await ws.ConnectAsync(BuildUrl(opts), ct);

        var pin = ResolvePin(opts);
        var requestedUuid = opts.ModuleInstanceUuid7 ?? "";

        var handshake = new Container
        {
            Version = 1,
            AuthToken = "",
            ModuleName = opts.ModuleName,
            ModuleInstanceUuid7 = requestedUuid,
            ConnectionRequest = new ConnectionRequest
            {
                Pin = pin,
                ProcessPosition = opts.ProcessPosition,
                Priority = opts.Priority,
                ModuleInstanceUuid7 = requestedUuid,
            },
        };

        await SendRawAsync(ws, handshake, ct);

        Container reply;
        try
        {
            reply = await ReceiveFrameAsync(ws, ct);
        }
        catch
        {
            ws.Dispose();
            throw;
        }

        if (reply.PayloadCase != Container.PayloadOneofCase.ConnectionRequestReturn)
        {
            ws.Dispose();
            throw new InvalidOperationException($"Authentication rejected by engine (expected connection_request_return, got {reply.PayloadCase}).");
        }

        var ret = reply.ConnectionRequestReturn;
        if (ret.NewPort != 0)
        {
            ws.Dispose();
            throw new InvalidOperationException($"Protocol error: engine requested a port hop (new_port={ret.NewPort}); the two-phase handshake is removed.");
        }

        if (string.IsNullOrEmpty(reply.AuthToken))
        {
            ws.Dispose();
            throw new InvalidOperationException("Authentication rejected by engine (no auth token).");
        }

        var assignedUuid = string.IsNullOrEmpty(ret.ModuleInstanceUuid7) ? requestedUuid : ret.ModuleInstanceUuid7;

        var client = new CockatielClient(opts, ws, reply.AuthToken, assignedUuid);
        client.StartReceiveLoop();
        return client;
    }

    /// <summary>
    /// Send any Container payload. The payload's runtime type is mapped
    /// automatically to the Container payload oneof field; unknown types throw.
    /// </summary>
    public async Task SendAsync<T>(T payload, CancellationToken ct = default) where T : class
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (payload is null) throw new ArgumentNullException(nameof(payload));
        if (!_payloadSetter.TryGetValue(payload.GetType(), out var prop))
            throw new ArgumentException($"{payload.GetType().Name} is not a valid Container payload type.", nameof(payload));

        var container = BuildContainer();
        prop.SetValue(container, payload);
        await SendLockedAsync(_ws, container, ct);
    }

    /// <summary>Register a handler for every inbound container.</summary>
    public void ReceiveAny(Action<Container> handler)
    {
        if (handler is null) throw new ArgumentNullException(nameof(handler));
        _allHandlers.Add(handler);
    }

    /// <summary>Register a typed handler for a specific payload type.</summary>
    public void On<T>(Action<T> handler) where T : class
    {
        if (handler is null) throw new ArgumentNullException(nameof(handler));
        var type = typeof(T);
        if (!_payloadSetter.ContainsKey(type))
            throw new ArgumentException($"{type.Name} is not a valid Container payload type.", nameof(handler));
        var list = _typedHandlers.GetOrAdd(type, _ => new List<Action<object>>());
        lock (list)
        {
            list.Add(payload => handler((T)payload));
        }
    }

    /// <summary>
    /// Drop the socket, open a fresh one, and send a Container carrying the
    /// stored JWT (contract §6). The engine recognizes the valid token as a
    /// reauth — no PIN needed.
    /// </summary>
    public async Task ReconnectAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (string.IsNullOrEmpty(_authToken))
            throw new InvalidOperationException("Cannot reconnect without an auth token.");

        var oldWs = _ws;
        var oldCts = _receiveCts;
        var oldTask = _receiveTask;

        // Open the fresh socket and send the reauth BEFORE retiring the old
        // one: the engine's reconnect path verifies the token against the
        // still-alive session, so it must exist when the reauth lands.
        var ws = new ClientWebSocket();
        await ws.ConnectAsync(BuildUrl(_opts), ct);

        // The engine requires the FIRST message on a fresh socket to be a
        // ConnectionRequest (main.rs:1429). A reauth is a ConnectionRequest
        // whose container carries the stored JWT as auth_token — the engine
        // verifies the token and resumes the session (no PIN needed, §6).
        var reauth = new Container
        {
            Version = 1,
            AuthToken = _authToken,
            ModuleName = _opts.ModuleName,
            ModuleInstanceUuid7 = _instanceUuid7,
            ConnectionRequest = new ConnectionRequest
            {
                Pin = 0, // ignored on reauth
                ProcessPosition = _opts.ProcessPosition,
                Priority = _opts.Priority,
                ModuleInstanceUuid7 = _instanceUuid7,
            },
        };
        await SendRawAsync(ws, reauth, ct);

        // Stop the old loop and retire the old socket (its pending receive is
        // aborted by the cancel — fine, it's being discarded).
        oldCts?.Cancel();
        if (oldTask is not null)
        {
            try { await oldTask; } catch { }
        }
        try { oldWs.Dispose(); } catch { }

        _ws = ws;
        StartReceiveLoop();
    }

    /// <summary>Gracefully close the connection (optionally announcing a reason).</summary>
    public async Task DisconnectAsync(string reason = "")
    {
        if (_disposed) return;
        _disposed = true;

        var ws = _ws;
        if (ws is null) return;

        if (!string.IsNullOrEmpty(reason) && ws.State == WebSocketState.Open)
        {
            try
            {
                var bye = new Container
                {
                    Version = 1,
                    AuthToken = _authToken,
                    ModuleName = _opts.ModuleName,
                    ModuleInstanceUuid7 = _instanceUuid7,
                    Shutdown = new Shutdown { Reason = reason },
                };
                await SendLockedAsync(ws, bye, CancellationToken.None);
            }
            catch { /* socket may already be closed */ }
        }

        // Send our close frame WITHOUT waiting for the reply, then let the
        // receive loop observe the peer's close reply and unwind on its own.
        // Cancelling a pending ReceiveAsync aborts the socket (no close
        // handshake), so it must come only after the loop has stopped.
        try
        {
            if (ws.State == WebSocketState.Open)
                await ws.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, reason ?? "", CancellationToken.None);
        }
        catch { }

        if (_receiveTask is not null)
        {
            try { await _receiveTask.WaitAsync(TimeSpan.FromSeconds(3)); } catch { }
        }

        _receiveCts?.Cancel();
        try { ws.Dispose(); } catch { }
    }

    /// <summary>Force-close without a graceful handshake.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _receiveCts?.Cancel();
        try { _ws?.Dispose(); } catch { }
        _sendLock.Dispose();
    }

    /// <summary>
    /// Load a module-local KEY=VALUE .env file into the process environment.
    /// Real environment variables always win. Auto-invoked by ConnectAsync.
    /// </summary>
    public static void LoadDotEnv(string? path = null)
    {
        if (path is null)
        {
            var cwd = Directory.GetCurrentDirectory();
            var local = Path.Combine(cwd, ".env");
            path = File.Exists(local) ? local : null;
        }
        if (path is null || !File.Exists(path)) return;

        foreach (var rawLine in File.ReadAllLines(path))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line.StartsWith('#')) continue;
            var eq = line.IndexOf('=');
            if (eq <= 0) continue;
            var key = line[..eq].Trim();
            var value = line[(eq + 1)..].Trim().Trim('"', '\'');
            if (key.Length == 0) continue;
            if (Environment.GetEnvironmentVariable(key) is null)
                Environment.SetEnvironmentVariable(key, value);
        }
    }

    private Container BuildContainer() => new Container
    {
        Version = 1,
        AuthToken = _authToken,
        ModuleName = _opts.ModuleName,
        ModuleInstanceUuid7 = _instanceUuid7,
    };

    private static Uri BuildUrl(CockatielClientOptions opts) => new($"ws://{opts.Ip}:{opts.Port}");

    private static int ResolvePin(CockatielClientOptions opts)
    {
        var env = Environment.GetEnvironmentVariable("COCKATIEL_PIN");
        if (int.TryParse(env, out var pin)) return pin;
        return opts.Pin;
    }

    private async Task SendLockedAsync(ClientWebSocket ws, Container container, CancellationToken ct)
    {
        await _sendLock.WaitAsync(ct);
        try { await SendRawAsync(ws, container, ct); }
        finally { _sendLock.Release(); }
    }

    private static async Task SendRawAsync(ClientWebSocket ws, Container container, CancellationToken ct)
    {
        var data = container.ToByteArray();
        await ws.SendAsync(data, WebSocketMessageType.Binary, true, ct);
    }

    private static async Task<Container> ReceiveFrameAsync(ClientWebSocket ws, CancellationToken ct)
    {
        var buffer = new byte[65536];
        using var ms = new MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await ws.ReceiveAsync(buffer, ct);
            if (result.MessageType == WebSocketMessageType.Close)
                throw new InvalidOperationException("Connection closed by engine during handshake.");
            if (result.MessageType == WebSocketMessageType.Binary)
                ms.Write(buffer, 0, result.Count);
        } while (!result.EndOfMessage);

        if (ms.Length == 0)
            throw new InvalidOperationException("Engine sent an empty frame during handshake.");

        return Container.Parser.ParseFrom(ms.ToArray());
    }

    private void StartReceiveLoop()
    {
        _receiveCts = new CancellationTokenSource();
        var cts = _receiveCts;
        _receiveTask = Task.Run(() => ReceiveLoopAsync(cts.Token));
    }

    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        var buffer = new byte[65536];
        var ms = new MemoryStream();

        while (!ct.IsCancellationRequested && !_disposed)
        {
            try
            {
                var ws = Volatile.Read(ref _ws);
                if (ws is null || ws.State != WebSocketState.Open) break;

                ms.SetLength(0);
                WebSocketReceiveResult result;
                do
                {
                    result = await ws.ReceiveAsync(buffer, ct);
                    if (result.MessageType == WebSocketMessageType.Close)
                        return; // ManagedWebSocket auto-replies; stop the loop here
                    if (result.MessageType == WebSocketMessageType.Binary)
                        ms.Write(buffer, 0, result.Count);
                } while (!result.EndOfMessage);

                if (ms.Length == 0) continue;

                Container container;
                try
                {
                    container = Container.Parser.ParseFrom(ms.ToArray());
                }
                catch
                {
                    continue; // malformed frames are ignored, never crash the loop
                }

                if (container.PayloadCase == Container.PayloadOneofCase.AuthVerify)
                {
                    // Answer the engine's liveness probe immediately on the same
                    // socket so a quiet module is never severed as "unresponsive".
                    await SendLockedAsync(_ws, new Container
                    {
                        Version = 1,
                        AuthToken = _authToken,
                        ModuleName = _opts.ModuleName,
                        ModuleInstanceUuid7 = _instanceUuid7,
                        AuthVerify = new AuthVerify { CurAuth = _authToken },
                    }, ct);
                    continue; // probes are control messages, not user-facing payloads
                }

                Dispatch(container);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                break; // socket faulted; stop the loop. Caller may ReconnectAsync().
            }
        }
    }

    private void Dispatch(Container container)
    {
        var payloadCase = container.PayloadCase;
        object? payload = null;
        if (payloadCase != Container.PayloadOneofCase.None && _payloadGetter.TryGetValue(payloadCase, out var getter))
            payload = getter.GetValue(container);

        // Slow handlers must never block the receive loop (which has to answer
        // liveness probes in time) — dispatch every handler on a background task.
        foreach (var handler in _allHandlers)
        {
            var h = handler;
            Task.Run(() => { try { h(container); } catch { /* user errors never break the loop */ } });
        }

        if (payload is null) return;
        var type = payload.GetType();
        if (!_typedHandlers.TryGetValue(type, out var list)) return;

        lock (list)
        {
            foreach (var handler in list)
            {
                var h = handler;
                Task.Run(() => { try { h(payload); } catch { } });
            }
        }
    }

    private static Dictionary<Type, PropertyInfo> BuildPayloadSetter()
    {
        var map = new Dictionary<Type, PropertyInfo>();
        foreach (var name in Enum.GetNames(typeof(Container.PayloadOneofCase)))
        {
            if (name == nameof(Container.PayloadOneofCase.None)) continue;
            var prop = typeof(Container).GetProperty(name);
            if (prop is not null)
                map[prop.PropertyType] = prop;
        }
        return map;
    }

    private static Dictionary<Container.PayloadOneofCase, PropertyInfo> BuildPayloadGetter()
    {
        var map = new Dictionary<Container.PayloadOneofCase, PropertyInfo>();
        foreach (var name in Enum.GetNames(typeof(Container.PayloadOneofCase)))
        {
            if (name == nameof(Container.PayloadOneofCase.None)) continue;
            var prop = typeof(Container).GetProperty(name);
            if (prop is not null)
                map[(Container.PayloadOneofCase)Enum.Parse(typeof(Container.PayloadOneofCase), name)] = prop;
        }
        return map;
    }
}

/// <summary>RFC 9562-style UUIDv7 generator (time-ordered).</summary>
public static class CockatielUuid7
{
    private static readonly object _lock = new();
    private static long _lastMilliseconds;
    private static int _counter;

    public static string NewUuid7()
    {
        lock (_lock)
        {
            var ms = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (ms < _lastMilliseconds) ms = _lastMilliseconds;
            else if (ms > _lastMilliseconds) { _lastMilliseconds = ms; _counter = 0; }
            var counter = _counter++ & 0xFFF;

            Span<byte> b = stackalloc byte[16];
            b[0] = (byte)(ms >> 40);
            b[1] = (byte)(ms >> 32);
            b[2] = (byte)(ms >> 24);
            b[3] = (byte)(ms >> 16);
            b[4] = (byte)(ms >> 8);
            b[5] = (byte)ms;
            b[6] = (byte)(0x70 | (counter >> 8)); // version 7 + 4 counter bits
            b[7] = (byte)counter;                 // 8 counter bits
            RandomNumberGenerator.Fill(b[8..]);
            b[8] = (byte)((b[8] & 0x3F) | 0x80);  // RFC 4122 variant

            return Format(b);
        }
    }

    private static string Format(ReadOnlySpan<byte> b)
    {
        Span<char> chars = stackalloc char[36];
        const string hex = "0123456789abcdef";
        var pos = 0;
        for (var i = 0; i < 16; i++)
        {
            if (i is 4 or 6 or 8 or 10) chars[pos++] = '-';
            chars[pos++] = hex[b[i] >> 4];
            chars[pos++] = hex[b[i] & 0xF];
        }
        return new string(chars);
    }
}