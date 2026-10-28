// MQTT-SN 1.2 gateway for TinyMQBroker over an Arduino UDP. The UDP is begun
// by the sketch on the gateway's port (1884 is customary).
//
//   WiFiUDP udp;
//   TinyMQSnListener sn(udp);
//   ...
//   udp.begin(1884);
//   sn.addPredefinedTopic(1, "sensors/temperature");
//   broker.addListener(sn);
//
// MQTT-SN has no encryption or authentication of its own: use it on a
// closed network or inside a TinyWG tunnel.
#pragma once

#include <IPAddress.h>
#include <Udp.h>

#include "TinyMQBroker.h"
#include "tinymq/mq_sn_gateway.h"

class TinyMQSnListener : public TinyMQListener, private tinymq::DatagramSender {
public:
    explicit TinyMQSnListener(UDP &udp) : udp_(udp) {}

    // `name` is not copied
    bool addPredefinedTopic(uint16_t id, const char *name) { return gateway_.addPredefinedTopic(id, name); }
    void setGatewayId(uint8_t id) { gateway_.setGatewayId(id); }
    // ADVERTISE every `seconds` to a broadcast address; 0 turns it off
    void setAdvertise(uint16_t seconds, IPAddress broadcast, uint16_t port);
    void setRetry(uint32_t ms, uint8_t count) { gateway_.setRetry(ms, count); }
    void setQosMinusOne(bool on) { gateway_.setQosMinusOne(on); }

    size_t clients() const { return gateway_.clients(); }
    tinymq::SnGateway &gateway() { return gateway_; }

    void update(tinymq::Broker &engine, uint32_t now) override;

private:
    void sendTo(const tinymq::Endpoint &to, const uint8_t *data, size_t len) override;

    UDP &udp_;
    tinymq::SnGateway gateway_;
    uint8_t buf_[TINYMQ_MAX_PACKET + 8];
};
