// Handwritten client. Spliced into java/Cockatiel.java by
// codegen/generate-cockatiel-java.sh (same package as the generated proto
// holder). The script hoists the import lines to the top of the file, so keep
// every import at column 0. Edit here and re-run the script, or edit
// Cockatiel.java directly — either stays in sync with this layout.
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Single-connection Java client for the Cockatiel chat engine.
 *
 * <p>Nested inside the generated {@link Cockatiel} holder (which owns the
 * protobuf types) so the whole library is one public source file. One
 * WebSocket carries the whole session: a ConnectionRequest (PIN) is answered
 * by a ConnectionRequestReturn carrying a JWT, and every later frame reuses
 * that JWT on the same socket. See CLIENT_CONTRACT.md.
 */
public static final class CockatielClient implements AutoCloseable {

    private static final int CONNECT_TIMEOUT_S = 10;
    private static final int SEND_TIMEOUT_S = 10;

    private static final Object SENTINEL_CLOSE = new Object();
    private static final Object SENTINEL_ERROR = new Object();

    /** Payload runtime type -> Container.Builder oneof setter. */
    private static final Map<Class<?>, BiConsumer<Container.Builder, Object>>
            PAYLOAD_SETTERS = buildPayloadSetters();
    /** Container oneof case -> payload getter. */
    private static final Map<Container.PayloadCase, Function<Container, Object>>
            PAYLOAD_GETTERS = buildPayloadGetters();

    /** KEY=VALUE from a module-local .env; real process env always wins. */
    private static final Map<String, String> DOTENV = new HashMap<>();
    private static boolean dotenvLoaded;

    private final CockatielClientOptions opts;
    private final HttpClient http;
    private final ExecutorService sendExecutor;

    private final List<Consumer<Container>> allHandlers =
            new CopyOnWriteArrayList<>();
    private final Map<Class<?>, List<Consumer<Object>>> typedHandlers = new ConcurrentHashMap<>();

    private volatile WebSocket ws;
    private volatile BlockingQueue<Object> frameQueue;
    private volatile String authToken;
    private volatile String instanceUuid7;
    private volatile Thread receiveThread;
    private volatile boolean disposed;

    private CockatielClient(CockatielClientOptions opts, HttpClient http, WebSocket ws,
                            BlockingQueue<Object> queue, String authToken, String instanceUuid7) {
        this.opts = opts;
        this.http = http;
        this.ws = ws;
        this.frameQueue = queue;
        this.authToken = authToken;
        this.instanceUuid7 = instanceUuid7;
        this.sendExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "cockatiel-send-" + label(opts.moduleName));
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Open one WebSocket and authenticate with the engine (single-connection
     * auth, contract §1). The PIN is read from COCKATIEL_PIN first, then
     * {@code opts.pin}, defaulting to 0. A module-local .env file is loaded
     * into the fallback environment first (real env vars win, §2).
     */
    public static CompletableFuture<CockatielClient> connectAsync(CockatielClientOptions opts) {
        return CompletableFuture.supplyAsync(() -> connect(opts));
    }

    private static CockatielClient connect(CockatielClientOptions opts) {
        if (opts == null) {
            throw new IllegalArgumentException("opts must not be null");
        }
        String moduleName = opts.moduleName == null ? "" : opts.moduleName.trim();
        if (moduleName.isEmpty() || moduleName.equals("unnamed_module")) {
            throw new IllegalArgumentException(
                    "ModuleName must be a non-blank identity; the engine rejects blank/unnamed_module names.");
        }
        loadDotEnv();
        int pin = resolvePin(opts);
        String requestedUuid = opts.moduleInstanceUuid7 == null ? "" : opts.moduleInstanceUuid7;

        WebSocket socket = null;
        try {
            BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
            HttpClient http = HttpClient.newHttpClient();
            socket = http.newWebSocketBuilder()
                    .buildAsync(URI.create(buildUrl(opts)), new FrameListener(queue))
                    .get(CONNECT_TIMEOUT_S, TimeUnit.SECONDS);

            Container handshake = Container.newBuilder()
                    .setVersion(1)
                    .setAuthToken("")
                    .setModuleName(moduleName)
                    .setModuleInstanceUuid7(requestedUuid)
                    .setConnectionRequest(ConnectionRequest.newBuilder()
                            .setPin(pin)
                            .setProcessPosition(opts.processPosition)
                            .setPriority(opts.priority)
                            .setModuleInstanceUuid7(requestedUuid))
                    .build();
            socket.sendBinary(ByteBuffer.wrap(handshake.toByteArray()), true)
                    .get(CONNECT_TIMEOUT_S, TimeUnit.SECONDS);

            Object item = queue.poll(CONNECT_TIMEOUT_S, TimeUnit.SECONDS);
            if (item == null || item == SENTINEL_CLOSE || item == SENTINEL_ERROR) {
                throw new IllegalStateException(
                        "Engine closed the socket before replying to the ConnectionRequest.");
            }
            Container reply =
                    Container.parseFrom((byte[]) item);

            if (reply.getPayloadCase()
                    != Container.PayloadCase.CONNECTION_REQUEST_RETURN) {
                throw new IllegalStateException("Authentication rejected by engine "
                        + "(expected connection_request_return, got " + reply.getPayloadCase() + ").");
            }
            ConnectionRequestReturn ret = reply.getConnectionRequestReturn();
            if (ret.getNewPort() != 0) {
                throw new IllegalStateException("Protocol error: engine requested a port hop "
                        + "(new_port=" + ret.getNewPort() + "); the two-phase handshake is removed.");
            }
            String jwt = reply.getAuthToken();
            if (jwt == null || jwt.isEmpty()) {
                throw new IllegalStateException("Authentication rejected by engine (no auth token).");
            }

            String assignedUuid = ret.getModuleInstanceUuid7().isEmpty()
                    ? requestedUuid : ret.getModuleInstanceUuid7();

            CockatielClient client =
                    new CockatielClient(opts, http, socket, queue, jwt, assignedUuid);
            client.startReceiveLoop();
            return client;
        } catch (java.util.concurrent.ExecutionException e) {
            socketAbort(socket);
            throw new IllegalStateException("Failed to connect to " + buildUrl(opts)
                    + ": " + rootCause(e), e);
        } catch (java.util.concurrent.TimeoutException e) {
            socketAbort(socket);
            throw new IllegalStateException("Timed out connecting to " + buildUrl(opts), e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            socketAbort(socket);
            throw new IllegalStateException("Malformed frame from engine during handshake", e);
        } catch (InterruptedException e) {
            socketAbort(socket);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while connecting", e);
        }
    }

    /**
     * Send any Container payload. The payload's runtime type is mapped
     * automatically to the Container payload oneof field; unknown types throw.
     */
    public CompletableFuture<Void> sendAsync(Object payload) {
        if (disposed) {
            return failedFuture(new IllegalStateException("client is disposed"));
        }
        if (payload == null) {
            return failedFuture(new IllegalArgumentException("payload must not be null"));
        }
        BiConsumer<Container.Builder, Object> setter =
                PAYLOAD_SETTERS.get(payload.getClass());
        if (setter == null) {
            return failedFuture(new IllegalArgumentException(
                    payload.getClass().getName() + " is not a valid Container payload type."));
        }
        return CompletableFuture.runAsync(() -> {
            try {
                Container.Builder builder = buildContainerBuilder();
                setter.accept(builder, payload);
                send(builder.build());
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, sendExecutor);
    }

    /** Register a handler for every inbound container. */
    public void onMessage(Consumer<Container> handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }
        allHandlers.add(handler);
    }

    /** Register a typed handler for a specific payload type. */
    public <T> void on(Class<T> type, Consumer<T> handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }
        if (!PAYLOAD_SETTERS.containsKey(type)) {
            throw new IllegalArgumentException(
                    type.getName() + " is not a valid Container payload type.");
        }
        typedHandlers.computeIfAbsent(type, k -> new CopyOnWriteArrayList<>())
                .add(payload -> handler.accept((T) payload));
    }

    /**
     * Drop the socket, open a fresh one, and send a Container carrying the
     * stored JWT (contract §6). The engine recognizes the valid token as a
     * reauth — no PIN needed. The first message on the fresh socket must be a
     * ConnectionRequest, whose container carries the JWT as auth_token.
     */
    public CompletableFuture<Void> reconnect() {
        if (disposed) {
            return failedFuture(new IllegalStateException("client is disposed"));
        }
        if (authToken == null || authToken.isEmpty()) {
            return failedFuture(new IllegalStateException("Cannot reconnect without an auth token."));
        }
        return CompletableFuture.runAsync(() -> {
            try {
                String jwt = authToken;
                String instance = instanceUuid7;
                BlockingQueue<Object> newQueue = new LinkedBlockingQueue<>();
                WebSocket socket = http.newWebSocketBuilder()
                        .buildAsync(URI.create(buildUrl(opts)), new FrameListener(newQueue))
                        .get(CONNECT_TIMEOUT_S, TimeUnit.SECONDS);

                Container reauth = buildContainerBuilder()
                        .setConnectionRequest(ConnectionRequest.newBuilder()
                                .setPin(0) // ignored on reauth
                                .setProcessPosition(opts.processPosition)
                                .setPriority(opts.priority)
                                .setModuleInstanceUuid7(instance))
                        .build();
                socket.sendBinary(ByteBuffer.wrap(reauth.toByteArray()), true)
                        .get(CONNECT_TIMEOUT_S, TimeUnit.SECONDS);

                // Retire the old socket AFTER the reauth lands: the engine
                // verifies the token against the still-alive session.
                Thread oldThread = receiveThread;
                if (oldThread != null) {
                    oldThread.interrupt();
                }
                WebSocket oldWs = ws;
                if (oldWs != null) {
                    try {
                        oldWs.abort();
                    } catch (Throwable ignored) {
                        // already closed
                    }
                }

                ws = socket;
                frameQueue = newQueue;
                startReceiveLoop();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, sendExecutor);
    }

    /**
     * Gracefully close the connection: send a close frame, then close the
     * socket and stop the receive loop.
     */
    public CompletableFuture<Void> disconnect() {
        if (disposed) {
            return CompletableFuture.completedFuture(null);
        }
        disposed = true;
        return CompletableFuture.runAsync(() -> {
            WebSocket s = ws;
            if (s != null) {
                try {
                    s.sendClose(WebSocket.NORMAL_CLOSURE, "bye").get(3, TimeUnit.SECONDS);
                } catch (Throwable ignored) {
                    // socket may already be closed
                }
                try {
                    s.abort();
                } catch (Throwable ignored) {
                    // already closed
                }
            }
            Thread t = receiveThread;
            if (t != null) {
                t.interrupt();
            }
            sendExecutor.shutdown();
        });
    }

    /** The engine-assigned JWT for this session. */
    public String authToken() {
        return authToken;
    }

    /** The engine-assigned (or requested) module instance uuid7. */
    public String moduleInstanceUuid7() {
        return instanceUuid7;
    }

    /** The module name this client authenticates as. */
    public String moduleName() {
        return opts.moduleName == null ? "" : opts.moduleName;
    }

    /** Force-close without a graceful handshake. */
    public void close() {
        try {
            disconnect().get(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // best effort
        }
    }

    /**
     * Load a module-local KEY=VALUE .env file. Java cannot mutate the real
     * process environment, so the values land in a fallback map that
     * {@code resolvePin} consults after the real {@code COCKATIEL_PIN}; real
     * environment variables always win. Auto-invoked by connectAsync.
     */
    public static void loadDotEnv() {
        loadDotEnv(null);
    }

    public static synchronized void loadDotEnv(String path) {
        if (dotenvLoaded) {
            return;
        }
        dotenvLoaded = true;
        if (path == null) {
            java.io.File local = new java.io.File(System.getProperty("user.dir", "."), ".env");
            path = local.exists() ? local.getAbsolutePath() : null;
        }
        if (path == null) {
            return;
        }
        java.io.File f = new java.io.File(path);
        if (!f.exists()) {
            return;
        }
        try {
            for (String rawLine : java.nio.file.Files.readAllLines(f.toPath())) {
                String line = rawLine.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                int eq = line.indexOf('=');
                if (eq <= 0) {
                    continue;
                }
                String key = line.substring(0, eq).trim();
                String value = line.substring(eq + 1).trim();
                if (value.length() >= 2
                        && ((value.startsWith("\"") && value.endsWith("\""))
                        || (value.startsWith("'") && value.endsWith("'")))) {
                    value = value.substring(1, value.length() - 1);
                }
                if (!key.isEmpty()) {
                    DOTENV.putIfAbsent(key, value);
                }
            }
        } catch (Exception ignored) {
            // a malformed .env must never block connecting
        }
    }

    private Container.Builder buildContainerBuilder() {
        return Container.newBuilder()
                .setVersion(1)
                .setAuthToken(authToken)
                .setModuleName(opts.moduleName == null ? "" : opts.moduleName)
                .setModuleInstanceUuid7(instanceUuid7);
    }

    private void send(Container container) throws Exception {
        WebSocket s = ws;
        if (s == null) {
            throw new IllegalStateException("not connected");
        }
        s.sendBinary(ByteBuffer.wrap(container.toByteArray()), true)
                .get(SEND_TIMEOUT_S, TimeUnit.SECONDS);
    }

    private void answerAuthVerify() {
        CompletableFuture.runAsync(() -> {
            try {
                Container.Builder builder = buildContainerBuilder();
                builder.setAuthVerify(AuthVerify.newBuilder()
                        .setCurAuth(authToken));
                send(builder.build());
            } catch (Exception ignored) {
                // the socket may be going away; nothing to do
            }
        }, sendExecutor);
    }

    private void startReceiveLoop() {
        Thread t = new Thread(this::receiveLoop,
                "cockatiel-receive-" + label(opts.moduleName));
        t.setDaemon(true);
        receiveThread = t;
        t.start();
    }

    private void receiveLoop() {
        BlockingQueue<Object> q = frameQueue;
        while (!disposed) {
            Object item;
            try {
                item = q.take();
            } catch (InterruptedException e) {
                break;
            }
            if (item == SENTINEL_CLOSE || item == SENTINEL_ERROR) {
                break;
            }
            Container container;
            try {
                container = Container.parseFrom((byte[]) item);
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                continue; // malformed frames are ignored, never crash the loop
            }

            if (container.getPayloadCase()
                    == Container.PayloadCase.AUTH_VERIFY) {
                // Answer the engine's liveness probe immediately on the same
                // socket so a quiet module is never severed as "unresponsive".
                answerAuthVerify();
                continue; // probes are control messages, not user-facing payloads
            }
            dispatch(container);
        }
    }

    private void dispatch(Container container) {
        // Slow handlers must never block the receive loop (which has to answer
        // liveness probes in time) — dispatch every handler as a background task.
        for (Consumer<Container> handler : allHandlers) {
            Consumer<Container> h = handler;
            CompletableFuture.runAsync(() -> {
                try {
                    h.accept(container);
                } catch (Throwable ignored) {
                    // user errors never break the loop
                }
            });
        }

        if (container.getPayloadCase() == Container.PayloadCase.PAYLOAD_NOT_SET) {
            return;
        }
        Function<Container, Object> getter = PAYLOAD_GETTERS.get(container.getPayloadCase());
        if (getter == null) {
            return;
        }
        Object payload = getter.apply(container);
        if (payload == null) {
            return;
        }
        List<Consumer<Object>> list = typedHandlers.get(payload.getClass());
        if (list == null) {
            return;
        }
        for (Consumer<Object> handler : list) {
            Consumer<Object> h = handler;
            CompletableFuture.runAsync(() -> {
                try {
                    h.accept(payload);
                } catch (Throwable ignored) {
                    // user errors never break the loop
                }
            });
        }
    }

    private static int resolvePin(CockatielClientOptions opts) {
        String env = System.getenv("COCKATIEL_PIN");
        if (env != null) {
            try {
                return Integer.parseInt(env.trim());
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        String dotenv = DOTENV.get("COCKATIEL_PIN");
        if (dotenv != null) {
            try {
                return Integer.parseInt(dotenv.trim());
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        return opts.pin;
    }

    private static String buildUrl(CockatielClientOptions opts) {
        return "ws://" + opts.ip + ":" + opts.port;
    }

    private static Map<Class<?>, BiConsumer<Container.Builder, Object>>
            buildPayloadSetters() {
        Map<Class<?>, BiConsumer<Container.Builder, Object>> map = new HashMap<>();
        for (Container.PayloadCase c : Container.PayloadCase.values()) {
            if (c == Container.PayloadCase.PAYLOAD_NOT_SET) {
                continue;
            }
            String methodName = "set" + camelCase(c.name());
            for (Method m : Container.Builder.class.getMethods()) {
                if (!m.getName().equals(methodName)) {
                    continue;
                }
                Class<?>[] params = m.getParameterTypes();
                if (params.length != 1) {
                    continue;
                }
                if (params[0].getName().endsWith("Builder")) {
                    continue; // pick the message overload, not the builder overload
                }
                map.put(params[0], (b, v) -> {
                    try {
                        m.invoke(b, v);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                break;
            }
        }
        return map;
    }

    private static Map<Container.PayloadCase, Function<Container, Object>>
            buildPayloadGetters() {
        Map<Container.PayloadCase, Function<Container, Object>> map =
                new HashMap<>();
        for (Container.PayloadCase c : Container.PayloadCase.values()) {
            if (c == Container.PayloadCase.PAYLOAD_NOT_SET) {
                continue;
            }
            String methodName = "get" + camelCase(c.name());
            try {
                Method m = Container.class.getMethod(methodName);
                map.put(c, (ct) -> {
                    try {
                        return m.invoke(ct);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (NoSuchMethodException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        return map;
    }

    private static String camelCase(String upperSnake) {
        StringBuilder sb = new StringBuilder(upperSnake.length());
        boolean upper = true;
        for (int i = 0; i < upperSnake.length(); i++) {
            char ch = upperSnake.charAt(i);
            if (ch == '_') {
                upper = true;
                continue;
            }
            sb.append(upper ? Character.toUpperCase(ch) : Character.toLowerCase(ch));
            upper = false;
        }
        return sb.toString();
    }

    private static String label(String moduleName) {
        return (moduleName == null || moduleName.isEmpty()) ? "unnamed" : moduleName;
    }

    private static String rootCause(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        String msg = c.getMessage();
        return msg == null || msg.isEmpty() ? c.getClass().getSimpleName() : msg;
    }

    private static void socketAbort(WebSocket socket) {
        if (socket != null) {
            try {
                socket.abort();
            } catch (Throwable ignored) {
                // best effort
            }
        }
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(t);
        return f;
    }

    private static final class FrameListener implements WebSocket.Listener {
        private final BlockingQueue<Object> queue;
        private ByteArrayOutputStream buf = new ByteArrayOutputStream();

        FrameListener(BlockingQueue<Object> queue) {
            this.queue = queue;
        }

        @Override
        public void onOpen(WebSocket ws) {
            ws.request(1);
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket ws, ByteBuffer data, boolean last) {
            byte[] b = new byte[data.remaining()];
            data.get(b);
            buf.write(b, 0, b.length);
            if (last) {
                queue.offer(buf.toByteArray());
                buf = new ByteArrayOutputStream();
            }
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            queue.offer(SENTINEL_CLOSE);
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            queue.offer(SENTINEL_ERROR);
        }
    }

    /**
     * RFC 9562-style UUIDv7 generator (time-ordered).
     */
    public static final class Uuid7 {
        private static final Object LOCK = new Object();
        private static long lastMs;
        private static int counter;
        private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();

        private Uuid7() {
        }

        public static String newUuid7() {
            synchronized (LOCK) {
                long ms = System.currentTimeMillis();
                if (ms < lastMs) {
                    ms = lastMs;
                } else if (ms > lastMs) {
                    lastMs = ms;
                    counter = 0;
                }
                int c = counter++ & 0xFFF;

                byte[] b = new byte[16];
                b[0] = (byte) (ms >> 40);
                b[1] = (byte) (ms >> 32);
                b[2] = (byte) (ms >> 24);
                b[3] = (byte) (ms >> 16);
                b[4] = (byte) (ms >> 8);
                b[5] = (byte) ms;
                b[6] = (byte) (0x70 | (c >> 8)); // version 7 + 4 counter bits
                b[7] = (byte) c;                 // 8 counter bits
                byte[] rnd = new byte[8];
                RANDOM.nextBytes(rnd);
                System.arraycopy(rnd, 0, b, 8, 8);
                b[8] = (byte) ((b[8] & 0x3F) | 0x80); // RFC 4122 variant

                StringBuilder sb = new StringBuilder(36);
                final char[] hex = "0123456789abcdef".toCharArray();
                for (int i = 0; i < 16; i++) {
                    if (i == 4 || i == 6 || i == 8 || i == 10) {
                        sb.append('-');
                    }
                    sb.append(hex[(b[i] >> 4) & 0xF]);
                    sb.append(hex[b[i] & 0xF]);
                }
                return sb.toString();
            }
        }
    }

    /**
     * Connection settings for {@link CockatielClient}.
     */
    public static class CockatielClientOptions {
        /** Engine host. Default 127.0.0.1. */
        public String ip = "127.0.0.1";
        /** Engine WebSocket port. Default 9734. */
        public int port = 9734;
        /** Engine PIN. Used only when COCKATIEL_PIN is not set; 0 = unset. */
        public int pin = 0;
        /** Module identity. The engine rejects blank / unnamed_module names. */
        public String moduleName = "";
        /** Pipeline position for the connection (default postprocess). */
        public ProcessPosition processPosition =
                ProcessPosition.PROCESS_POSITION_POSTPROCESS;
        /** Connection priority (default 100). */
        public int priority = 100;
        /** Requested instance uuid7 on first connect; the engine assigns one if empty. */
        public String moduleInstanceUuid7 = "";
    }
}