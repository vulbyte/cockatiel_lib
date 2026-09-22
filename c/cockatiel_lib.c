#include "cockatiel_lib.h"

#include <libwebsockets.h>
#include <pb_decode.h>
#include <pb_encode.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ------------------------------------------------------------------ */
/* Internal state                                                      */
/* ------------------------------------------------------------------ */

enum {
    TX_QUEUE_CAP = 32,
};

typedef struct tx_frame {
    uint8_t *data; /* malloc'd, COCKATIEL_MAX_FRAME + LWS_PRE headroom */
    size_t len;
} tx_frame;

struct cockatiel_client {
    /* connection */
    struct lws_context *context;
    struct lws *wsi;
    char url[256];
    char host[128];
    int port;
    char path[64];
    int use_ssl;

    /* identity */
    char module_name[128];
    int process_position;
    uint32_t priority;

    /* auth */
    int pin;
    char auth_token[512];        /* JWT after handshake */
    char module_instance_uuid7[64]; /* engine-assigned instance id */
    int authed;
    int reconnecting;            /* 1: handshake carries stored JWT */

    /* run control */
    volatile int closing;
    volatile int connected;
    int handshake_done;
    int handshake_ok;

    /* send queue */
    tx_frame tx[TX_QUEUE_CAP];
    int tx_head;
    int tx_tail;

    /* user callback */
    cockatiel_on_container on_container;
    void *userdata;

    char errbuf[256];
};

/* ------------------------------------------------------------------ */
/* libwebsockets plumbing                                              */
/* ------------------------------------------------------------------ */

static int cockatiel_callback(struct lws *wsi, enum lws_callback_reasons reason,
                              void *user, void *in, size_t len);

static const struct lws_protocols cockatiel_protocols[] = {
    {
        "cockatiel",
        cockatiel_callback,
        sizeof(cockatiel_client *),
        COCKATIEL_MAX_FRAME,
        0,
        NULL,
        0,
    },
    LWS_PROTOCOL_LIST_TERM,
};

/* ------------------------------------------------------------------ */
/* Payload table: oneof tag -> struct size + descriptor               */
/* ------------------------------------------------------------------ */

typedef struct {
    uint32_t tag;
    size_t size;
    const pb_msgdesc_t *desc;
} payload_entry;

#define P(tag_, type_) \
    { (tag_), sizeof(cockatiel_protobuf_v1_##type_), &cockatiel_protobuf_v1_##type_##_msg }

static const payload_entry PAYLOAD_TABLE[] = {
    P(COCKATIEL_PAYLOAD_CONNECTION_REQUEST, ConnectionRequest),
    P(COCKATIEL_PAYLOAD_CONNECTION_REQUEST_RETURN, ConnectionRequestReturn),
    P(COCKATIEL_PAYLOAD_AUTH_VERIFY, AuthVerify),
    P(COCKATIEL_PAYLOAD_AUTH_NEW, AuthNew),
    P(COCKATIEL_PAYLOAD_COMMAND, Command),
    P(COCKATIEL_PAYLOAD_COMMANDS, Commands),
    P(COCKATIEL_PAYLOAD_MESSAGE_PRE_PROCESS, MessagePreProcess),
    P(COCKATIEL_PAYLOAD_MESSAGE_IN_PROCESS, MessageInProcess),
    P(COCKATIEL_PAYLOAD_MESSAGE_POST_PROCESS, MessagePostProcess),
    P(COCKATIEL_PAYLOAD_TIMELINE_EVENT, TimelineEvent),
    P(COCKATIEL_PAYLOAD_USER_DATA, UserData),
    P(COCKATIEL_PAYLOAD_SHUTDOWN, Shutdown),
    P(COCKATIEL_PAYLOAD_LOG, Log),
    P(COCKATIEL_PAYLOAD_ERR, Err),
    P(COCKATIEL_PAYLOAD_SEND_TO_PLATFORMS, SendToPlatforms),
    P(COCKATIEL_PAYLOAD_MESSAGE_ACK, MessageAck),
    P(COCKATIEL_PAYLOAD_DATABASE_QUERY, DatabaseQuery),
    P(COCKATIEL_PAYLOAD_DATABASE_QUERY_RESULT, DatabaseQueryResult),
    P(COCKATIEL_PAYLOAD_MODULE_CONTROL, ModuleControl),
    P(COCKATIEL_PAYLOAD_MODULE_CONTROL_RESULT, ModuleControlResult),
    P(COCKATIEL_PAYLOAD_PROMPT, Prompt),
    P(COCKATIEL_PAYLOAD_PROMPT_RESPONSE, PromptResponse),
    P(COCKATIEL_PAYLOAD_AUDIT_FLAG, AuditFlag),
};

static const payload_entry *payload_lookup(uint32_t tag) {
    for (size_t i = 0; i < sizeof(PAYLOAD_TABLE) / sizeof(PAYLOAD_TABLE[0]); i++) {
        if (PAYLOAD_TABLE[i].tag == tag) return &PAYLOAD_TABLE[i];
    }
    return NULL;
}

const char *cockatiel_payload_name(uint32_t tag) {
    static const char *names[] = {
        "connection_request",       "connection_request_return",
        "auth_verify",              "auth_new",
        "command_payload",          "commands_payload",
        "message_pre_process",      "message_in_process",
        "message_post_process",     "timeline_event",
        "user_data",                "shutdown",
        "log",                      "err",
        "send_to_platforms",        "message_ack",
        "database_query",           "database_query_result",
        "module_control",           "module_control_result",
        "prompt",                   "prompt_response",
        "audit_flag",
    };
    static const uint32_t tags[] = {
        COCKATIEL_PAYLOAD_CONNECTION_REQUEST,
        COCKATIEL_PAYLOAD_CONNECTION_REQUEST_RETURN,
        COCKATIEL_PAYLOAD_AUTH_VERIFY,
        COCKATIEL_PAYLOAD_AUTH_NEW,
        COCKATIEL_PAYLOAD_COMMAND,
        COCKATIEL_PAYLOAD_COMMANDS,
        COCKATIEL_PAYLOAD_MESSAGE_PRE_PROCESS,
        COCKATIEL_PAYLOAD_MESSAGE_IN_PROCESS,
        COCKATIEL_PAYLOAD_MESSAGE_POST_PROCESS,
        COCKATIEL_PAYLOAD_TIMELINE_EVENT,
        COCKATIEL_PAYLOAD_USER_DATA,
        COCKATIEL_PAYLOAD_SHUTDOWN,
        COCKATIEL_PAYLOAD_LOG,
        COCKATIEL_PAYLOAD_ERR,
        COCKATIEL_PAYLOAD_SEND_TO_PLATFORMS,
        COCKATIEL_PAYLOAD_MESSAGE_ACK,
        COCKATIEL_PAYLOAD_DATABASE_QUERY,
        COCKATIEL_PAYLOAD_DATABASE_QUERY_RESULT,
        COCKATIEL_PAYLOAD_MODULE_CONTROL,
        COCKATIEL_PAYLOAD_MODULE_CONTROL_RESULT,
        COCKATIEL_PAYLOAD_PROMPT,
        COCKATIEL_PAYLOAD_PROMPT_RESPONSE,
        COCKATIEL_PAYLOAD_AUDIT_FLAG,
    };
    for (size_t i = 0; i < sizeof(tags) / sizeof(tags[0]); i++) {
        if (tags[i] == tag) return names[i];
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static int url_parse(const char *url, char *host, int *port, char *path,
                     int *use_ssl) {
    const char *p = url;
    int ssl = 0;
    if (strncmp(p, "wss://", 6) == 0) {
        ssl = 1;
        p += 6;
    } else if (strncmp(p, "ws://", 5) == 0) {
        p += 5;
    } else {
        return -1;
    }

    const char *host_start = p;
    while (*p && *p != ':' && *p != '/') p++;
    size_t host_len = (size_t)(p - host_start);
    if (host_len == 0 || host_len >= 128) return -1;
    memcpy(host, host_start, host_len);
    host[host_len] = '\0';

    int port_val = ssl ? 443 : 9734;
    if (*p == ':') {
        p++;
        port_val = 0;
        while (*p >= '0' && *p <= '9') {
            port_val = port_val * 10 + (*p - '0');
            p++;
        }
        if (port_val <= 0 || port_val > 65535) return -1;
    }

    const char *path_start = (*p == '/') ? p : "/";
    if (strncmp(path_start, "/", 2) == 0) {
        strncpy(path, "/", 64);
    } else {
        strncpy(path, path_start, 63);
        path[63] = '\0';
    }

    *port = port_val;
    *use_ssl = ssl;
    return 0;
}

/* ------------------------------------------------------------------ */
/* UUID7                                                               */
/* ------------------------------------------------------------------ */

void cockatiel_uuid7(char out[37]) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    uint64_t ms = (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;

    uint8_t b[16];
    uint64_t r = ((uint64_t)rand() << 32) ^ (uint64_t)rand();

    /* 48-bit big-endian unix timestamp (ms). */
    b[0] = (uint8_t)(ms >> 40);
    b[1] = (uint8_t)(ms >> 32);
    b[2] = (uint8_t)(ms >> 24);
    b[3] = (uint8_t)(ms >> 16);
    b[4] = (uint8_t)(ms >> 8);
    b[5] = (uint8_t)ms;

    /* 12 bits of rand_a: nibble in byte6 low, byte7 full. */
    b[6] = (uint8_t)(0x70 | ((r >> 8) & 0x0F)); /* version 7 */
    b[7] = (uint8_t)(r & 0xFF);

    /* 62 bits of rand_b, variant 10 in byte8 top bits. */
    b[8] = (uint8_t)(0x80 | ((r >> 56) & 0x3F));
    b[9] = (uint8_t)(r >> 48);
    b[10] = (uint8_t)(r >> 40);
    b[11] = (uint8_t)(r >> 32);
    b[12] = (uint8_t)(r >> 24);
    b[13] = (uint8_t)(r >> 16);
    b[14] = (uint8_t)(r >> 8);
    b[15] = (uint8_t)r;

    snprintf(out, 37,
             "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10],
             b[11], b[12], b[13], b[14], b[15]);
}

/* ------------------------------------------------------------------ */
/* Frame encode/decode                                                 */
/* ------------------------------------------------------------------ */

static int encode_container(const cockatiel_protobuf_v1_Container *c,
                            uint8_t *buf, size_t cap, size_t *out_len) {
    pb_ostream_t stream = pb_ostream_from_buffer(buf, cap);
    if (!pb_encode(&stream, &cockatiel_protobuf_v1_Container_msg, c)) {
        return -1;
    }
    *out_len = stream.bytes_written;
    return 0;
}

static int decode_container(const uint8_t *buf, size_t len,
                            cockatiel_protobuf_v1_Container *out) {
    pb_istream_t stream = pb_istream_from_buffer(buf, len);
    if (!pb_decode(&stream, &cockatiel_protobuf_v1_Container_msg, out)) {
        return -1;
    }
    return 0;
}

/* Build the envelope used by every outbound container. */
static void fill_envelope(cockatiel_client *c, cockatiel_protobuf_v1_Container *out) {
    memset(out, 0, sizeof(*out));
    out->version = 1;
    snprintf(out->auth_token, sizeof(out->auth_token), "%s", c->auth_token);
    snprintf(out->module_name, sizeof(out->module_name), "%s", c->module_name);
    snprintf(out->module_instance_uuid7, sizeof(out->module_instance_uuid7), "%s",
             c->module_instance_uuid7);
}

/* Queue a fully-encoded frame for transmission. */
static int tx_queue_push(cockatiel_client *c, const uint8_t *data, size_t len) {
    int next = (c->tx_tail + 1) % TX_QUEUE_CAP;
    if (next == c->tx_head) return -1; /* full */

    uint8_t *buf = malloc(LWS_PRE + COCKATIEL_MAX_FRAME);
    if (!buf) return -1;
    memcpy(buf + LWS_PRE, data, len);
    c->tx[c->tx_tail].data = buf;
    c->tx[c->tx_tail].len = len;
    c->tx_tail = next;
    return 0;
}

static void tx_queue_flush(cockatiel_client *c) {
    while (c->tx_head != c->tx_tail && c->wsi) {
        tx_frame *f = &c->tx[c->tx_head];
        if (f->len == 0) {
            free(f->data);
            f->data = NULL;
            c->tx_head = (c->tx_head + 1) % TX_QUEUE_CAP;
            continue;
        }
        int written = lws_write(c->wsi, f->data + LWS_PRE, f->len, LWS_WRITE_BINARY);
        if (written < (int)f->len) {
            /* Socket broken; drop the rest of the queue. */
            while (c->tx_head != c->tx_tail) {
                free(c->tx[c->tx_head].data);
                c->tx[c->tx_head].data = NULL;
                c->tx[c->tx_head].len = 0;
                c->tx_head = (c->tx_head + 1) % TX_QUEUE_CAP;
            }
            break;
        }
        free(f->data);
        f->data = NULL;
        f->len = 0;
        c->tx_head = (c->tx_head + 1) % TX_QUEUE_CAP;
    }
    if (c->tx_head != c->tx_tail && c->wsi) {
        lws_callback_on_writable(c->wsi);
    }
}

static int send_frame(cockatiel_client *c, cockatiel_protobuf_v1_Container *container) {
    uint8_t buf[COCKATIEL_MAX_FRAME];
    size_t len = 0;
    if (encode_container(container, buf, sizeof(buf), &len) != 0) return -1;
    if (tx_queue_push(c, buf, len) != 0) return -1;
    if (c->wsi) lws_callback_on_writable(c->wsi);
    return 0;
}

/* ------------------------------------------------------------------ */
/* AuthVerify auto-answer                                              */
/* ------------------------------------------------------------------ */

static int answer_auth_verify(cockatiel_client *c) {
    if (!c->authed) return 0; /* nothing to prove yet */
    cockatiel_protobuf_v1_Container container;
    fill_envelope(c, &container);
    container.which_payload = COCKATIEL_PAYLOAD_AUTH_VERIFY;
    snprintf(container.payload.auth_verify.cur_auth,
             sizeof(container.payload.auth_verify.cur_auth), "%s", c->auth_token);
    return send_frame(c, &container);
}

/* ------------------------------------------------------------------ */
/* Connection handshake                                                */
/* ------------------------------------------------------------------ */

static int send_connection_request(cockatiel_client *c) {
    cockatiel_protobuf_v1_Container container;
    fill_envelope(c, &container);
    container.which_payload = COCKATIEL_PAYLOAD_CONNECTION_REQUEST;
    container.payload.connection_request.pin = c->pin;
    container.payload.connection_request.process_position =
        (cockatiel_protobuf_v1_ProcessPosition)c->process_position;
    container.payload.connection_request.priority = c->priority;
    /* module_instance_uuid7 empty on first connect; engine assigns */
    return send_frame(c, &container);
}

/* Reconnect: fresh socket carrying the stored JWT (no PIN). The engine
 * requires the first message on a fresh connection to be a ConnectionRequest;
 * a non-empty auth_token marks it as a reauth and skips the PIN check. */
static int send_reauth(cockatiel_client *c) {
    cockatiel_protobuf_v1_Container container;
    fill_envelope(c, &container);
    container.which_payload = COCKATIEL_PAYLOAD_CONNECTION_REQUEST;
    container.payload.connection_request.pin = 0;
    container.payload.connection_request.process_position =
        (cockatiel_protobuf_v1_ProcessPosition)c->process_position;
    container.payload.connection_request.priority = c->priority;
    snprintf(container.payload.connection_request.module_instance_uuid7,
             sizeof(container.payload.connection_request.module_instance_uuid7),
             "%s", c->module_instance_uuid7);
    return send_frame(c, &container);
}

/* ------------------------------------------------------------------ */
/* libwebsockets callback                                              */
/* ------------------------------------------------------------------ */

static int cockatiel_callback(struct lws *wsi, enum lws_callback_reasons reason,
                              void *user, void *in, size_t len) {
    cockatiel_client *c = (cockatiel_client *)user;
    (void)wsi;
    if (!c) return 0;

    switch (reason) {
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
            c->connected = 0;
            c->handshake_done = 1;
            c->handshake_ok = 0;
            break;

        case LWS_CALLBACK_CLIENT_ESTABLISHED:
            c->connected = 1;
            /* lws_service() sleeps until an event and ignores its timeout;
             * arm a periodic timer so the receive loop can observe
             * cockatiel_stop()/disconnect without waiting on socket I/O. */
            lws_set_timer_usecs(wsi, 100000); /* 100ms */
            if (c->reconnecting) {
                /* Reauth: the engine replies with no ConnectionRequestReturn;
                 * a live socket carrying the JWT is accepted. Any rejection
                 * surfaces as an immediate close. */
                send_reauth(c);
                c->handshake_done = 1;
                c->handshake_ok = 1;
            } else {
                send_connection_request(c);
            }
            break;

        case LWS_CALLBACK_TIMER:
            lws_set_timer_usecs(wsi, 100000); /* re-arm */
            break;

        case LWS_CALLBACK_CLIENT_RECEIVE: {
            if (len == 0 || !in) break;
            cockatiel_protobuf_v1_Container container;
            memset(&container, 0, sizeof(container));
            if (decode_container((const uint8_t *)in, len, &container) != 0) {
                break; /* malformed frame: skip */
            }

            uint32_t tag = container.which_payload;

            /* Liveness probe: answer immediately, never dispatch to user. */
            if (tag == COCKATIEL_PAYLOAD_AUTH_VERIFY) {
                answer_auth_verify(c);
                break;
            }

            /* Handshake response: capture JWT + assigned instance id. */
            if (tag == COCKATIEL_PAYLOAD_CONNECTION_REQUEST_RETURN && !c->authed) {
                const cockatiel_protobuf_v1_ConnectionRequestReturn *ret =
                    &container.payload.connection_request_return;
                if (ret->new_port != 0) {
                    c->handshake_done = 1;
                    c->handshake_ok = 0;
                    break; /* removed two-phase flow: protocol error */
                }
                if (container.auth_token[0] == '\0') {
                    c->handshake_done = 1;
                    c->handshake_ok = 0;
                    break;
                }
                snprintf(c->auth_token, sizeof(c->auth_token), "%s",
                         container.auth_token);
                if (ret->module_instance_uuid7[0] != '\0') {
                    snprintf(c->module_instance_uuid7,
                             sizeof(c->module_instance_uuid7), "%s",
                             ret->module_instance_uuid7);
                } else {
                    snprintf(c->module_instance_uuid7,
                             sizeof(c->module_instance_uuid7), "%s",
                             container.module_instance_uuid7);
                }
                c->authed = 1;
                c->handshake_done = 1;
                c->handshake_ok = 1;
                break;
            }

            /* Reconnect ack: a live socket carrying our JWT is accepted with
             * no ConnectionRequestReturn; authed already set. */
            if (c->reconnecting && c->authed) {
                c->handshake_done = 1;
                c->handshake_ok = 1;
                break;
            }

            if (c->authed && c->on_container) {
                c->on_container(c, &container, c->userdata);
            }
            break;
        }

        case LWS_CALLBACK_CLIENT_WRITEABLE:
            tx_queue_flush(c);
            break;

        case LWS_CALLBACK_CLIENT_CLOSED:
            c->connected = 0;
            if (!c->handshake_done) {
                c->handshake_done = 1;
                c->handshake_ok = 0;
            }
            break;

        default:
            break;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Context lifecycle                                                   */
/* ------------------------------------------------------------------ */

static int create_context(cockatiel_client *c) {
    struct lws_context_creation_info info;
    memset(&info, 0, sizeof(info));
    info.port = CONTEXT_PORT_NO_LISTEN;
    info.protocols = cockatiel_protocols;
    info.gid = -1;
    info.uid = -1;
    if (c->use_ssl) {
        info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    }
    c->context = lws_create_context(&info);
    return c->context ? 0 : -1;
}

static int open_socket(cockatiel_client *c) {
    struct lws_client_connect_info i;
    memset(&i, 0, sizeof(i));
    i.context = c->context;
    i.address = c->host;
    i.port = c->port;
    i.path = c->path;
    i.host = c->host;
    i.origin = c->host;
    i.protocol = cockatiel_protocols[0].name;
    i.userdata = c;
    i.ssl_connection = c->use_ssl ? LCCSCF_USE_SSL : 0;
    c->wsi = lws_client_connect_via_info(&i);
    return c->wsi ? 0 : -1;
}

/* ------------------------------------------------------------------ */
/* Public API                                                          */
/* ------------------------------------------------------------------ */

cockatiel_client *cockatiel_connect(const char *url, int pin,
                                    const char *module_name,
                                    int process_position, uint32_t priority,
                                    char *errbuf, size_t errbuf_len) {
    if (!url || !module_name || !module_name[0]) return NULL;

    /* PIN precedence #1: COCKATIEL_PIN env wins over the argument. */
    const char *env_pin = getenv("COCKATIEL_PIN");
    if (env_pin && env_pin[0]) {
        pin = atoi(env_pin);
    }

    cockatiel_client *c = calloc(1, sizeof(*c));
    if (!c) return NULL;

    if (url_parse(url, c->host, &c->port, c->path, &c->use_ssl) != 0) {
        free(c);
        return NULL;
    }
    snprintf(c->url, sizeof(c->url), "%s", url);
    snprintf(c->module_name, sizeof(c->module_name), "%s", module_name);
    c->pin = pin;
    c->process_position = process_position;
    c->priority = priority;
    c->reconnecting = 0;
    c->closing = 0;
    c->connected = 0;

    srand((unsigned)time(NULL) ^ (unsigned)(uintptr_t)c);

    if (create_context(c) != 0) {
        snprintf(c->errbuf, sizeof(c->errbuf), "failed to create lws context");
        goto fail;
    }
    if (open_socket(c) != 0) {
        snprintf(c->errbuf, sizeof(c->errbuf), "failed to open ws connection to %s", url);
        goto fail;
    }

    /* Pump the event loop until the ConnectionRequestReturn arrives. */
    int timeout_ms = 10000;
    while (!c->handshake_done && !c->closing && timeout_ms > 0) {
        lws_service(c->context, 50);
        timeout_ms -= 50;
    }

    if (!c->handshake_ok || !c->authed) {
        snprintf(c->errbuf, sizeof(c->errbuf), "authentication rejected by engine");
        goto fail;
    }

    if (errbuf && errbuf_len) errbuf[0] = '\0';
    return c;

fail:
    if (errbuf && errbuf_len) {
        snprintf(errbuf, errbuf_len, "%s", c->errbuf[0] ? c->errbuf : "connect failed");
    }
    if (c->context) {
        if (c->wsi) lws_set_timeout(c->wsi, NO_PENDING_TIMEOUT, LWS_TO_KILL_ASYNC);
        lws_cancel_service(c->context);
        lws_context_destroy(c->context);
    }
    free(c);
    return NULL;
}

int cockatiel_send(cockatiel_client *c, cockatiel_payload payload_field,
                   const void *message) {
    if (!c || !c->authed || !c->connected || c->closing) return -1;
    const payload_entry *pe = payload_lookup((uint32_t)payload_field);
    if (!pe || !message) return -1;

    cockatiel_protobuf_v1_Container container;
    fill_envelope(c, &container);
    container.which_payload = pe->tag;
    memcpy(&container.payload, message, pe->size);
    return send_frame(c, &container);
}

int cockatiel_receive_loop(cockatiel_client *c, cockatiel_on_container cb,
                           void *userdata) {
    if (!c) return -1;
    c->on_container = cb;
    c->userdata = userdata;

    while (!c->closing && c->connected) {
        lws_service(c->context, 100);
    }
    return c->connected ? 0 : -1;
}

int cockatiel_reconnect(cockatiel_client *c) {
    if (!c || !c->authed) return -1;

    if (c->wsi) {
        lws_set_timeout(c->wsi, NO_PENDING_TIMEOUT, LWS_TO_KILL_ASYNC);
        c->wsi = NULL;
    }
    c->connected = 0;
    c->handshake_done = 0;
    c->handshake_ok = 0;
    c->reconnecting = 1;

    if (create_context(c) != 0) {
        c->reconnecting = 0;
        return -1;
    }
    if (open_socket(c) != 0) {
        c->reconnecting = 0;
        return -1;
    }

    int timeout_ms = 10000;
    while (!c->handshake_done && !c->closing && timeout_ms > 0) {
        lws_service(c->context, 0);
        timeout_ms -= 50;
    }
    c->reconnecting = 0;

    if (!c->handshake_ok || !c->connected) return -1;

    /* Reauth is confirmed by the socket staying open; give the engine a
     * moment to sever us if the JWT was rejected before declaring success. */
    if (c->authed) {
        struct timespec start, now;
        clock_gettime(CLOCK_REALTIME, &start);
        do {
            lws_service(c->context, 0);
            clock_gettime(CLOCK_REALTIME, &now);
        } while (c->connected && !c->closing &&
                 (now.tv_sec - start.tv_sec) * 1000000000L +
                         (now.tv_nsec - start.tv_nsec) <
                     200000000L);
        if (!c->connected) return -1;
    }
    return 0;
}

void cockatiel_stop(cockatiel_client *c) {
    if (!c) return;
    c->closing = 1;
    /* lws_service() can block for 10-30s when the socket is idle and ignores
     * its own timeout argument; cancel_service wakes it up from any thread. */
    if (c->context) lws_cancel_service(c->context);
}

void cockatiel_disconnect(cockatiel_client *c) {
    if (!c) return;
    c->closing = 1;
    if (c->context) lws_cancel_service(c->context);
    if (c->wsi) {
        lws_set_timeout(c->wsi, NO_PENDING_TIMEOUT, LWS_TO_KILL_ASYNC);
        c->wsi = NULL;
    }
    if (c->context) {
        lws_context_destroy(c->context);
        c->context = NULL;
    }
    for (int i = 0; i < TX_QUEUE_CAP; i++) {
        free(c->tx[i].data);
        c->tx[i].data = NULL;
    }
    free(c);
}