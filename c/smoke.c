/*
 * smoke.c — live smoke test for the C cockatiel client.
 *
 * Connects to the engine as "cockatiel-test-runner" (auto-approved), sends a
 * Log payload, then pumps the receive loop, printing every inbound container.
 * The engine broadcasts engine log lines back to connected modules as Log
 * payloads, so the smoke test sees its own "Auto-approving trusted module"
 * broadcast arrive.
 *
 * Usage:
 *   COCKATIEL_PIN=<pin> ./cockatiel_smoke [ws://host:port]
 */

#include <cockatiel_lib.h>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int g_frames = 0;
static int g_chain_verified = 0;
static const char *g_chain_qid = "c_chain_check";

static void on_container(cockatiel_client *client,
                         const cockatiel_protobuf_v1_Container *container,
                         void *userdata) {
    (void)client;
    (void)userdata;
    g_frames++;
    const char *name = cockatiel_payload_name(container->which_payload);
    printf("  [RX #%d] payload=%s", g_frames, name ? name : "?");

    switch (container->which_payload) {
        case COCKATIEL_PAYLOAD_LOG:
            printf("  log=\"%s\"", container->payload.log.log);
            break;
        case COCKATIEL_PAYLOAD_DATABASE_QUERY_RESULT:
            printf("  query_id=\"%s\" success=%d blob=%d",
                   container->payload.database_query_result.query_id,
                   container->payload.database_query_result.success,
                   (int)container->payload.database_query_result.result_blob.size);
            if (strcmp(container->payload.database_query_result.query_id, g_chain_qid) == 0 &&
                container->payload.database_query_result.success &&
                container->payload.database_query_result.result_blob.size > 0) {
                g_chain_verified = 1;
            }
            break;
        case COCKATIEL_PAYLOAD_CONNECTION_REQUEST_RETURN:
            printf("  new_port=%u uuid7=%s",
                   container->payload.connection_request_return.new_port,
                   container->payload.connection_request_return.module_instance_uuid7);
            break;
        case COCKATIEL_PAYLOAD_AUTH_NEW:
            printf("  new_auth=\"%s\"", container->payload.auth_new.new_auth);
            break;
        case COCKATIEL_PAYLOAD_SHUTDOWN:
            printf("  reason=\"%s\"", container->payload.shutdown.reason);
            break;
        case COCKATIEL_PAYLOAD_ERR:
            printf("  log=\"%s\"", container->payload.err.log);
            break;
        default:
            break;
    }
    printf("\n");
}

static void *receive_thread(void *arg) {
    cockatiel_client *client = (cockatiel_client *)arg;
    int rc = cockatiel_receive_loop(client, on_container, NULL);
    printf("  [receive loop returned rc=%d]\n", rc);
    return NULL;
}

int main(int argc, char **argv) {
    setvbuf(stdout, NULL, _IOLBF, 0); /* keep progress visible when piped */
    const char *url = (argc > 1) ? argv[1] : "ws://127.0.0.1:9734";
    const char *env_pin = getenv("COCKATIEL_PIN");
    int pin = env_pin ? atoi(env_pin) : 0;

    char errbuf[256] = {0};
    printf("cockatiel C smoke test\n");
    printf("  url:       %s\n", url);
    printf("  module:    cockatiel-test-runner\n");
    printf("  position:  postprocess (3)\n");
    printf("  priority:  10\n");
    printf("  pin:       %s\n", env_pin ? "from COCKATIEL_PIN" : "(0)");

    cockatiel_client *client = cockatiel_connect(
        url, pin, "cockatiel-test-runner", COCKATIEL_POSITION_POSTPROCESS, 10,
        errbuf, sizeof(errbuf));
    if (!client) {
        fprintf(stderr, "connect failed: %s\n", errbuf);
        return 1;
    }
    printf("[OK] connected & authenticated\n");

    /* The engine drains up to 32 pre-authorization frames right after issuing
     * the token; give it a beat to settle before our first real payload. */
    struct timespec settle = {.tv_sec = 0, .tv_nsec = 200000000L};
    nanosleep(&settle, NULL);

    /* Send a Log payload. */
    cockatiel_protobuf_v1_Log log = cockatiel_protobuf_v1_Log_init_zero;
    snprintf(log.log, sizeof(log.log),
             "C client smoke test @ %ld", (long)time(NULL));
    if (cockatiel_send(client, COCKATIEL_PAYLOAD_LOG, &log) != 0) {
        fprintf(stderr, "send failed\n");
        cockatiel_disconnect(client);
        return 1;
    }
    printf("[OK] sent Log: \"%s\"\n", log.log);

    /* A fresh uuid7 for a message id, just to exercise the generator. */
    char uuid[37];
    cockatiel_uuid7(uuid);
    printf("[OK] generated uuid7: %s\n", uuid);

    /* ── Chain dataflow: ingest as an adapter (empty message_uuid7), then
     *    verify the timeline row via a DatabaseQuery. ── */
    char msg[128];
    snprintf(msg, sizeof(msg), "C chain message %ld", (long)time(NULL));
    cockatiel_protobuf_v1_MessagePreProcess pre = cockatiel_protobuf_v1_MessagePreProcess_init_zero;
    pre.has_raw_message = true;
    snprintf(pre.raw_message.platform, sizeof(pre.raw_message.platform), "test");
    snprintf(pre.raw_message.raw_message, sizeof(pre.raw_message.raw_message), "%s", msg);
    /* message_uuid7 stays "" so the engine ingests as a brand-new message. */
    if (cockatiel_send(client, COCKATIEL_PAYLOAD_MESSAGE_PRE_PROCESS, &pre) != 0) {
        fprintf(stderr, "ingest send failed\n");
        cockatiel_disconnect(client);
        return 1;
    }
    printf("[OK] ingested: %s\n", msg);

    struct timespec ingest_wait = {.tv_sec = 0, .tv_nsec = 150000000L};
    nanosleep(&ingest_wait, NULL);

    cockatiel_protobuf_v1_DatabaseQuery q = cockatiel_protobuf_v1_DatabaseQuery_init_zero;
    snprintf(q.query_id, sizeof(q.query_id), "%s", g_chain_qid);
    snprintf(q.sql, sizeof(q.sql),
             "SELECT pipeline_status FROM timeline_events WHERE platform = 'test' AND raw_message = '%s'", msg);
    if (cockatiel_send(client, COCKATIEL_PAYLOAD_DATABASE_QUERY, &q) != 0) {
        fprintf(stderr, "query send failed\n");
        cockatiel_disconnect(client);
        return 1;
    }
    printf("[OK] sent chain verify query\n");

    /* Pump frames for a couple of seconds, printing what arrives. */
    printf("receive loop (5s)...\n");
    pthread_t tid;
    if (pthread_create(&tid, NULL, receive_thread, client) != 0) {
        fprintf(stderr, "pthread_create failed\n");
        cockatiel_disconnect(client);
        return 1;
    }
    struct timespec sleep_ts = {.tv_sec = 5, .tv_nsec = 0};
    nanosleep(&sleep_ts, NULL);
    cockatiel_stop(client);
    pthread_join(tid, NULL);

    printf("[OK] received %d frame(s)\n", g_frames);
    printf("[%s] chain dataflow\n", g_chain_verified ? "CHAIN_OK" : "CHAIN_FAILED");
    cockatiel_disconnect(client);
    printf("done.\n");
    return g_chain_verified ? 0 : 1;
}