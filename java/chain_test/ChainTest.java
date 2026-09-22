package cockatiel;

import com.google.protobuf.ByteString;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Live chain test for the Java Cockatiel client.
 *
 * <p>Connects to an isolated engine as {@code cockatiel-test-runner}
 * (auto-approved), ingests a brand-new message via {@code MessagePreProcess}
 * (empty message_uuid7, platform "test"), then verifies the timeline row with a
 * {@code DatabaseQuery}. Prints CHAIN_OK when the result has a row and exits 0.
 *
 * <p>Usage:
 * <pre>java -cp .:lib/protobuf-java-4.35.1.jar cockatiel.ChainTest [ws://host:port] [pin]</pre>
 * The pin defaults to COCKATIEL_PIN (env) then 123456.
 */
public class ChainTest {

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "ws://127.0.0.1:9737";
        int pin = 123456;
        String envPin = System.getenv("COCKATIEL_PIN");
        if (envPin != null && !envPin.trim().isEmpty()) {
            try {
                pin = Integer.parseInt(envPin.trim());
            } catch (NumberFormatException ignored) {
                // keep default
            }
        }

        java.net.URI uri = java.net.URI.create(url);
        Cockatiel.CockatielClient.CockatielClientOptions opts =
                new Cockatiel.CockatielClient.CockatielClientOptions();
        opts.ip = uri.getHost();
        opts.port = uri.getPort();
        opts.pin = pin;
        opts.moduleName = "cockatiel-test-runner";
        opts.priority = 10;

        System.out.println("cockatiel Java chain test");
        System.out.println("  url:      " + url);
        System.out.println("  module:   " + opts.moduleName);
        System.out.println("  position: postprocess (3)");
        System.out.println("  priority: " + opts.priority);
        System.out.println("  pin:      " + (System.getenv("COCKATIEL_PIN") != null
                ? "from COCKATIEL_PIN" : String.valueOf(pin)));

        Cockatiel.CockatielClient client =
                Cockatiel.CockatielClient.connectAsync(opts).get(10, TimeUnit.SECONDS);
        System.out.println("[OK] connected & authenticated");
        System.out.println("[OK] module_instance_uuid7=" + client.moduleInstanceUuid7());

        // The engine drains up to 32 pre-authorization frames right after
        // issuing the token; give it a beat to settle before our first payload.
        Thread.sleep(200);
        System.out.println("[OK] generated uuid7: " + Cockatiel.CockatielClient.Uuid7.newUuid7());

        CompletableFuture<Boolean> verified = new CompletableFuture<>();
        client.onMessage(c -> System.out.println("  [RX] payload=" + c.getPayloadCase()));
        client.on(Cockatiel.Log.class,
                l -> System.out.println("  [RX] log=\"" + l.getLog() + "\""));
        client.on(Cockatiel.DatabaseQueryResult.class, r -> {
            String blob = r.getResultBlob().toStringUtf8();
            System.out.println("  [RX] database_query_result query_id=" + r.getQueryId()
                    + " success=" + r.getSuccess() + " blob=" + blob);
            if (!verified.isDone() && r.getSuccess() && hasRow(r.getResultBlob())) {
                verified.complete(true);
            }
        });

        String msg = "java chain message " + System.currentTimeMillis();
        Cockatiel.MessagePreProcess pre = Cockatiel.MessagePreProcess.newBuilder()
                .setMessageUuid7("")
                .setRawMessage(Cockatiel.ChatMessage.newBuilder()
                        .setPlatform("test")
                        .setRawMessage(msg))
                .build();
        client.sendAsync(pre).get(5, TimeUnit.SECONDS);
        System.out.println("[OK] ingested: " + msg);

        Thread.sleep(150);

        String qid = Cockatiel.CockatielClient.Uuid7.newUuid7();
        Cockatiel.DatabaseQuery query = Cockatiel.DatabaseQuery.newBuilder()
                .setQueryId(qid)
                .setSql("SELECT pipeline_status FROM timeline_events WHERE platform = 'test'"
                        + " AND raw_message = '" + msg + "'")
                .build();
        client.sendAsync(query).get(5, TimeUnit.SECONDS);
        System.out.println("[OK] sent chain verify query: " + query.getSql());

        boolean ok;
        try {
            ok = verified.get(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            ok = false;
            System.out.println("[TIMEOUT] no database_query_result within 5s");
        }

        System.out.println("[" + (ok ? "CHAIN_OK" : "CHAIN_FAILED") + "] chain dataflow");
        client.disconnect().get(5, TimeUnit.SECONDS);
        System.exit(ok ? 0 : 1);
    }

    /** A result blob is a JSON array of row objects; an empty result is "[]". */
    private static boolean hasRow(ByteString blob) {
        String json = blob.toStringUtf8();
        return json != null && json.indexOf('{') >= 0;
    }
}