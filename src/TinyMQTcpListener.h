// TCP listener for TinyMQBroker over an Arduino server: WiFiServer,
// NetworkServer, EthernetServer and the like. The server is begun by the
// sketch; the listener takes new clients from accept() (or available() on
// cores without it).
//
//   WiFiServer server(1883);
//   TinyMQTcpListener<WiFiServer> listener(server);
//   ...
//   server.begin();
//   broker.addListener(listener);
#pragma once

#include <utility>

#include "TinyMQBroker.h"

namespace tinymq_detail {
template <class S>
auto acceptClient(S &s, int) -> decltype(s.accept())
{
    return s.accept();
}
template <class S>
auto acceptClient(S &s, long) -> decltype(s.available())
{
    return s.available();
}
}  // namespace tinymq_detail

template <class ServerT, size_t MaxClients = TINYMQ_BROKER_MAX_CLIENTS>
class TinyMQTcpListener : public TinyMQListener {
public:
    typedef decltype(tinymq_detail::acceptClient(std::declval<ServerT &>(), 0)) ClientT;

    explicit TinyMQTcpListener(ServerT &server) : server_(server) {}

    void update(tinymq::Broker &engine, uint32_t now) override
    {
        for (int k = 0; k < 4; ++k) {
            ClientT c = tinymq_detail::acceptClient(server_, 0);
            if (!c)
                break;
            Slot *slot = freeSlot(engine);
            if (!slot) {
                c.stop();
                continue;
            }
            slot->client = c;
            slot->conn = engine.accept(*slot, now);
            if (slot->conn < 0)
                slot->client.stop();
        }
        for (Slot &s : slots_) {
            if (s.conn < 0)
                continue;
            if (!engine.owns(s.conn, s)) {
                s.conn = -1;  // the engine closed it
                continue;
            }
            for (int i = 0; i < 8; ++i) {
                int avail = s.client.available();
                if (avail <= 0)
                    break;
                size_t room;
                uint8_t *p = engine.rxBuffer(s.conn, room);
                if (room == 0)
                    break;
                int n = s.client.read(p, (size_t)avail < room ? (size_t)avail : room);
                if (n <= 0)
                    break;
                engine.rxCommit(s.conn, (size_t)n, now);
                if (!engine.owns(s.conn, s))
                    break;
            }
            if (engine.owns(s.conn, s) && !s.client.connected() && s.client.available() <= 0)
                engine.linkLost(s.conn);
            if (!engine.owns(s.conn, s))
                s.conn = -1;
        }
    }

private:
    struct Slot : tinymq::Link {
        ClientT client;
        int conn = -1;

        size_t write(const uint8_t *data, size_t len) override
        {
            size_t done = 0;
            while (done < len) {
                size_t n = client.write(data + done, len - done);
                if (n == 0)
                    break;
                done += n;
            }
            return done;
        }
        void close() override { client.stop(); }
    };

    Slot *freeSlot(tinymq::Broker &engine)
    {
        for (Slot &s : slots_)
            if (s.conn < 0 || !engine.owns(s.conn, s)) {
                s.conn = -1;
                return &s;
            }
        return nullptr;
    }

    ServerT &server_;
    Slot slots_[MaxClients];
};
