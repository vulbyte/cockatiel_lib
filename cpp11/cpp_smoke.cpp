/*
 * cpp_smoke.cpp — C++ wrapper live smoke: connect → send Log → receive.
 *
 *   c++ -std=c++11 cpp_smoke.cpp -I. -I../c -I../c/nanopb ../c/build/libcockatiel_lib.a \
 *       $(pkg-config --cflags --libs libwebsockets) -o cpp_smoke
 *   COCKATIEL_PIN=<pin> ./cpp_smoke
 */

#include <cockatiel_lib.hpp>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <thread>

int main() {
    const char *env_pin = getenv("COCKATIEL_PIN");
    int pin = env_pin ? atoi(env_pin) : 0;

    try {
        cockatiel::Client client;
        client.connect("ws://127.0.0.1:9734", pin, "cockatiel-test-runner",
                       COCKATIEL_POSITION_POSTPROCESS, 10);
        std::printf("[OK] connected & authenticated\n");

        std::atomic<int> frames{0};
        std::thread rx([&] {
            client.receive_loop([&](const cockatiel::Container *c) {
                const char *name = cockatiel_payload_name(c->which_payload);
                std::printf("[RX] payload=%s\n", name ? name : "?");
                frames++;
            });
        });

        cockatiel_protobuf_v1_Log log = cockatiel_protobuf_v1_Log_init_zero;
        std::snprintf(log.log, sizeof(log.log), "C++ client smoke test");
        client.send(COCKATIEL_PAYLOAD_LOG, &log);
        std::printf("[OK] sent Log; uuid7=%s\n", cockatiel::uuid7().c_str());

        for (int i = 0; i < 30; i++) { // ~3s
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        client.stop();
        rx.join();
        std::printf("[OK] received %d frame(s)\ndone.\n", frames.load());
        return 0;
    } catch (const std::exception &e) {
        std::fprintf(stderr, "C++ client error: %s\n", e.what());
        return 1;
    }
}