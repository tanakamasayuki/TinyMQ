#include "TinyMQClientSn.h"

void TinyMQClientSn::setGateway(IPAddress ip, uint16_t port)
{
    gateway_ = ip;
    port_ = port;
}

bool TinyMQClientSn::publishPredefined(uint16_t topicId, const char *payload, int8_t qos, bool retain)
{
    return engine_.publishPredefined(topicId, reinterpret_cast<const uint8_t *>(payload), payload ? strlen(payload) : 0, qos, retain);
}

bool TinyMQClientSn::publishShort(const char *topic, const char *payload, int8_t qos, bool retain)
{
    return engine_.publishShort(topic, reinterpret_cast<const uint8_t *>(payload), payload ? strlen(payload) : 0, qos, retain);
}

#if TINYMQ_SN_CLIENT_LEVEL >= 1
bool TinyMQClientSn::connect(const char *clientId)
{
    engine_.setClientId(clientId);
    return engine_.connect();
}

bool TinyMQClientSn::publish(const char *topic, const char *payload, uint8_t qos, bool retain)
{
    return engine_.publish(topic, reinterpret_cast<const uint8_t *>(payload), payload ? strlen(payload) : 0, qos, retain);
}
#endif

#if TINYMQ_SN_CLIENT_LEVEL >= 2
void TinyMQClientSn::setWill(const char *topic, const char *payload, uint8_t qos, bool retain)
{
    engine_.setWill(topic, reinterpret_cast<const uint8_t *>(payload), payload ? strlen(payload) : 0, qos, retain);
}

int TinyMQClientSn::searchGateway(IPAddress broadcast, uint16_t port, uint8_t radius, uint32_t timeoutMs)
{
    broadcast_ = broadcast;
    broadcastPort_ = port;
    return engine_.searchGateway(radius, timeoutMs);
}
#endif

bool TinyMQClientSn::send(const uint8_t *data, size_t len)
{
    if (!udp_.beginPacket(gateway_, port_))
        return false;
    udp_.write(data, len);
    return udp_.endPacket() == 1;
}

size_t TinyMQClientSn::receive(uint8_t *buf, size_t cap)
{
    int n = udp_.parsePacket();
    if (n <= 0)
        return 0;
    if ((size_t)n > cap) {
        udp_.flush();
        return 0;
    }
    lastIp_ = udp_.remoteIP();
    lastPort_ = udp_.remotePort();
    int got = udp_.read(buf, (size_t)n);
    return got > 0 ? (size_t)got : 0;
}

void TinyMQClientSn::idle()
{
    if (onIdle_)
        onIdle_(onIdleCtx_);
    delay(1);
}

bool TinyMQClientSn::broadcast(const uint8_t *data, size_t len)
{
    if (!udp_.beginPacket(broadcast_, broadcastPort_))
        return false;
    udp_.write(data, len);
    return udp_.endPacket() == 1;
}

void TinyMQClientSn::useLastSender()
{
    gateway_ = lastIp_;
    port_ = lastPort_;
}
