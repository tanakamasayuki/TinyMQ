// TCP listener for TinyMQBroker on lwIP's raw API, for cores with lwIP
// (ESP32, arduino-pico, CH32V with the Network library). Lighter than
// TinyMQTcpListener: no socket per client, data handed over from lwIP's
// buffers, and writable() from the TCP send buffer, so the broker holds
// messages back instead of blocking when a client reads slowly.
//
//   TinyMQLwipListener listener(1883);
//   ...                                 // after the network is up
//   listener.begin();
//   broker.addListener(listener);
//
// On CH32V, include <Network.h> before this (it brings lwIP into the build).
//
// lwIP's callbacks only queue what arrives; the broker runs from update()
// in loop(), under the lwIP lock where the core has one.
#pragma once

// Not in arduino-pico's FreeRTOS mode, where lwIP runs in its own task.
#if __has_include(<lwip/tcp.h>) && !(defined(ARDUINO_ARCH_RP2040) && defined(__FREERTOS))
#define TINYMQ_HAS_LWIP 1

#include <lwip/tcp.h>

#include "TinyMQBroker.h"

class TinyMQLwipListener : public TinyMQListener {
public:
    explicit TinyMQLwipListener(uint16_t port) : port_(port) {}

    // Opens the listening socket. False if lwIP refused (port in use, no memory).
    bool begin();
    void end();

    void update(tinymq::Broker &engine, uint32_t now) override;

private:
    struct Slot : tinymq::Link {
        TinyMQLwipListener *owner = nullptr;
        tcp_pcb *pcb = nullptr;
        pbuf *rx = nullptr;     // received, not yet given to the broker
        int conn = -1;
        bool fresh = false;     // accepted, not yet given to the broker
        bool gone = false;      // closed by the peer or by an error

        size_t write(const uint8_t *data, size_t len) override;
        void close() override;
        size_t writable() override;
        void release();         // forgets the pcb and the queued data
    };

    static err_t onAccept(void *arg, tcp_pcb *pcb, err_t err);
    static err_t onRecv(void *arg, tcp_pcb *pcb, pbuf *p, err_t err);
    static void onError(void *arg, err_t err);

    uint16_t port_;
    tcp_pcb *listen_ = nullptr;
    Slot slots_[TINYMQ_BROKER_MAX_CLIENTS];
};

#endif  // lwIP
