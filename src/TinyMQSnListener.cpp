#include "TinyMQSnListener.h"

static tinymq::Endpoint toEndpoint(IPAddress ip, uint16_t port)
{
    tinymq::Endpoint e;
    for (int i = 0; i < 4; ++i)
        e.ip[i] = ip[i];
    e.port = port;
    return e;
}

void TinyMQSnListener::setAdvertise(uint16_t seconds, IPAddress broadcast, uint16_t port)
{
    gateway_.setAdvertise(seconds, toEndpoint(broadcast, port));
}

void TinyMQSnListener::update(tinymq::Broker &engine, uint32_t now)
{
    if (!gateway_.attached())
        gateway_.begin(engine, *this);
    for (int k = 0; k < 8; ++k) {
        int n = udp_.parsePacket();
        if (n <= 0)
            break;
        if ((size_t)n > sizeof(buf_)) {
            udp_.flush();  // larger than any message the gateway takes
            continue;
        }
        int got = udp_.read(buf_, (size_t)n);
        if (got <= 0)
            continue;
        gateway_.input(toEndpoint(udp_.remoteIP(), udp_.remotePort()), buf_, (size_t)got, now);
    }
    gateway_.poll(now);
}

void TinyMQSnListener::sendTo(const tinymq::Endpoint &to, const uint8_t *data, size_t len)
{
    udp_.beginPacket(IPAddress(to.ip[0], to.ip[1], to.ip[2], to.ip[3]), to.port);
    udp_.write(data, len);
    udp_.endPacket();
}
