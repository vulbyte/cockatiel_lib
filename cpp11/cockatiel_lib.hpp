#ifndef COCKATIEL_LIB_HPP
#define COCKATIEL_LIB_HPP
/*
 * cockatiel_lib.hpp — thin C++11 RAII wrapper over the C client (c/).
 *
 * One-line import:
 *   #include <cockatiel_lib.hpp>
 *   #include <cockatiel_lib.h>
 * Link: the c/ static lib + libwebsockets + protobuf-c runtime (nanopb vendored).
 */

#include <cockatiel_lib.h>

#include <functional>
#include <stdexcept>
#include <string>

namespace cockatiel {

using Container = cockatiel_protobuf_v1_Container;
using OnContainer = std::function<void(const Container *)>;

/// RAII handle to a Cockatiel engine connection (single-connection PIN→JWT
/// auth — see CLIENT_CONTRACT.md).
class Client {
  public:
    Client() = default;
    Client(const Client &) = delete;
    Client &operator=(const Client &) = delete;

    Client(Client &&other) noexcept : _c(other._c) { other._c = nullptr; }
    Client &operator=(Client &&other) noexcept {
        if (this != &other) {
            close();
            _c = other._c;
            other._c = nullptr;
        }
        return *this;
    }

    ~Client() { close(); }

    /// Connect to the engine. `pin` may be 0 when COCKATIEL_PIN is set.
    /// Throws std::runtime_error on failure.
    void connect(const std::string &url, int pin, const std::string &module_name,
                 int process_position, int priority) {
        char errbuf[256] = {0};
        _c = cockatiel_connect(url.c_str(), pin, module_name.c_str(),
                               process_position, priority, errbuf, sizeof(errbuf));
        if (!_c) throw std::runtime_error(errbuf);
    }

    /// Wrap `message` in a Container and send it. Throws on failure.
    void send(cockatiel_payload payload_field, const void *message) {
        if (cockatiel_send(_c, payload_field, message) != 0)
            throw std::runtime_error("cockatiel_send failed");
    }

    /// Run the receive loop until the socket closes or stop() is called.
    /// The loop auto-answers AuthVerify liveness probes.
    void receive_loop(const OnContainer &on_container) {
        _cb = on_container;
        cockatiel_receive_loop(
            _c,
            [](cockatiel_client *c, const Container *container, void *ud) {
                auto *self = static_cast<Client *>(ud);
                if (self->_cb) self->_cb(container);
                (void)c;
            },
            this);
    }

    /// Ask a running receive_loop() to return at its next opportunity.
    void stop() { cockatiel_stop(_c); }

    /// Reconnect with the stored JWT (fresh socket + ConnectionRequest).
    void reconnect() {
        if (cockatiel_reconnect(_c) != 0)
            throw std::runtime_error("cockatiel_reconnect failed");
    }

    void close() {
        if (_c) {
            cockatiel_disconnect(_c);
            _c = nullptr;
        }
        _cb = nullptr;
    }

    cockatiel_client *native() const { return _c; }

  private:
    cockatiel_client *_c = nullptr;
    OnContainer _cb;
};

/// RFC 9562 UUIDv7 helper.
inline std::string uuid7() {
    char buf[37];
    cockatiel_uuid7(buf);
    return std::string(buf);
}

} // namespace cockatiel

#endif // COCKATIEL_LIB_HPP