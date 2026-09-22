#ifndef COCKATIEL_LIB_H
#define COCKATIEL_LIB_H

#include <stddef.h>
#include <stdint.h>
#include "cockatiel_protobuf.pb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Largest single frame the client will encode/decode. The engine's biggest
 * realistic payload is a DatabaseQueryResult result_blob (64 KiB); 256 KiB
 * gives headroom for nested ChatMessage + audio payloads. */
#define COCKATIEL_MAX_FRAME (256u * 1024u)

/* Container.payload oneof field numbers (see cockatiel_protobuf.proto). */
typedef enum {
    COCKATIEL_PAYLOAD_NONE = 0,
    COCKATIEL_PAYLOAD_CONNECTION_REQUEST = 7,
    COCKATIEL_PAYLOAD_CONNECTION_REQUEST_RETURN = 8,
    COCKATIEL_PAYLOAD_AUTH_VERIFY = 9,
    COCKATIEL_PAYLOAD_AUTH_NEW = 10,
    COCKATIEL_PAYLOAD_COMMAND = 11,
    COCKATIEL_PAYLOAD_COMMANDS = 12,
    COCKATIEL_PAYLOAD_MESSAGE_PRE_PROCESS = 13,
    COCKATIEL_PAYLOAD_MESSAGE_IN_PROCESS = 14,
    COCKATIEL_PAYLOAD_MESSAGE_POST_PROCESS = 15,
    COCKATIEL_PAYLOAD_TIMELINE_EVENT = 16,
    COCKATIEL_PAYLOAD_USER_DATA = 17,
    COCKATIEL_PAYLOAD_SHUTDOWN = 18,
    COCKATIEL_PAYLOAD_LOG = 19,
    COCKATIEL_PAYLOAD_ERR = 20,
    COCKATIEL_PAYLOAD_SEND_TO_PLATFORMS = 21,
    COCKATIEL_PAYLOAD_MESSAGE_ACK = 22,
    COCKATIEL_PAYLOAD_DATABASE_QUERY = 23,
    COCKATIEL_PAYLOAD_DATABASE_QUERY_RESULT = 24,
    COCKATIEL_PAYLOAD_MODULE_CONTROL = 25,
    COCKATIEL_PAYLOAD_MODULE_CONTROL_RESULT = 26,
    COCKATIEL_PAYLOAD_PROMPT = 27,
    COCKATIEL_PAYLOAD_PROMPT_RESPONSE = 28,
    COCKATIEL_PAYLOAD_AUDIT_FLAG = 29
} cockatiel_payload;

/* ProcessPosition enum values (see cockatiel_protobuf.proto). */
enum {
    COCKATIEL_POSITION_UNSPECIFIED = 0,
    COCKATIEL_POSITION_PREPROCESS = 1,
    COCKATIEL_POSITION_INPROCESS = 2,
    COCKATIEL_POSITION_POSTPROCESS = 3,
    COCKATIEL_POSITION_CONNECTION = 4
};

typedef struct cockatiel_client cockatiel_client;

/* Called for every inbound container whose payload is NOT the auth handshake.
 * `container` is a fully-decoded Container; inspect `which_payload` for the
 * active oneof member. Return quickly — the receive loop does not block on you. */
typedef void (*cockatiel_on_container)(cockatiel_client *client,
                                       const cockatiel_protobuf_v1_Container *container,
                                       void *userdata);

/* Single-connection auth: opens ONE WebSocket to `url` (e.g. "ws://127.0.0.1:9734"),
 * sends a ConnectionRequest carrying the PIN, reads the ConnectionRequestReturn,
 * keeps the same socket and stores the returned JWT. If COCKATIEL_PIN is set in
 * the environment it wins over the `pin` argument (contract PIN precedence #1).
 * Returns a client handle, or NULL with a message in errbuf (optional). */
cockatiel_client *cockatiel_connect(const char *url,
                                    int pin,
                                    const char *module_name,
                                    int process_position,
                                    uint32_t priority,
                                    char *errbuf,
                                    size_t errbuf_len);

/* Wraps `message` in a Container (version=1, current JWT, module identity) and
 * queues it on the live socket. `payload_field` must be one of the
 * COCKATIEL_PAYLOAD_* values; the message pointer must point at the matching
 * generated struct. Returns 0 on success, non-zero on error. */
int cockatiel_send(cockatiel_client *client,
                   cockatiel_payload payload_field,
                   const void *message);

/* Blocking receive loop. Decodes every inbound frame into a Container,
 * automatically answers AuthVerify liveness probes (replying with our JWT),
 * and invokes `cb` for every other payload. Malformed frames are skipped.
 * Returns when the socket closes, cockatiel_disconnect() is called, or an
 * unrecoverable error occurs. */
int cockatiel_receive_loop(cockatiel_client *client,
                           cockatiel_on_container cb,
                           void *userdata);

/* Drops the socket and opens a fresh one carrying the stored JWT (reauth).
 * PIN is not needed. Returns 0 on success, non-zero on failure. */
int cockatiel_reconnect(cockatiel_client *client);

/* Asks a running cockatiel_receive_loop() to return at its next opportunity.
 * Safe to call from another thread or from the receive callback. Does NOT
 * close the socket or free the client. */
void cockatiel_stop(cockatiel_client *client);

/* Closes the socket and frees the client. Safe to call from the callback. */
void cockatiel_disconnect(cockatiel_client *client);

/* RFC 9562 UUIDv7 (time-ordered) formatted as a 36-char lowercase string.
 * `out` must be at least 37 bytes. */
void cockatiel_uuid7(char out[37]);

/* Human-readable name for a payload tag (e.g. "log"), or NULL. */
const char *cockatiel_payload_name(uint32_t tag);

#ifdef __cplusplus
}
#endif

#endif /* COCKATIEL_LIB_H */