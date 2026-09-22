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
    cockatiel_disconnect(client);
    printf("done.\n");
    return 0;
}