// MQTT-SN 1.2 client over any Arduino UDP, for chips too small for TCP.
// The UDP is begun by the sketch (any local port).
//
//   WiFiUDP udp;
//   TinyMQClientSn sn(udp);
//   ...
//   udp.begin(0);
//   sn.setGateway(IPAddress(192, 168, 1, 10), 1884);
//   sn.publishPredefined(1, "21.5");   // QoS -1: no connection needed
//
// Procedures block until the gateway answers (see tinymq/mq_sn_client.h).
// The features are chosen with TINYMQ_SN_CLIENT_LEVEL.
#pragma once

#include <Arduino.h>
#include <IPAddress.h>
#include <Udp.h>

#include "tinymq/mq_sn_client.h"

class TinyMQClientSn : private tinymq::SnTransport {
public:
    typedef tinymq::SnClientEngine::MessageHandler MessageHandler;
    // Called repeatedly while a procedure waits for the gateway
    typedef void (*IdleHandler)(void *context);

    explicit TinyMQClientSn(UDP &udp) : udp_(udp), engine_(*this) {}

    void setGateway(IPAddress ip, uint16_t port = 1884);
    void onIdle(IdleHandler h, void *context = nullptr) { onIdle_ = h; onIdleCtx_ = context; }

    // QoS -1 (default) needs no connection
    bool publishPredefined(uint16_t topicId, const uint8_t *data, size_t len, int8_t qos = -1, bool retain = false) { return engine_.publishPredefined(topicId, data, len, qos, retain); }
    bool publishPredefined(uint16_t topicId, const char *payload, int8_t qos = -1, bool retain = false);
    bool publishShort(const char *topic, const uint8_t *data, size_t len, int8_t qos = -1, bool retain = false) { return engine_.publishShort(topic, data, len, qos, retain); }
    bool publishShort(const char *topic, const char *payload, int8_t qos = -1, bool retain = false);

#if TINYMQ_SN_CLIENT_LEVEL >= 1
    void setClientId(const char *id) { engine_.setClientId(id); }
    void setKeepAlive(uint16_t seconds) { engine_.setKeepAlive(seconds); }
    void setCleanSession(bool clean) { engine_.setCleanSession(clean); }
    void setRetry(uint32_t ms, uint8_t count) { engine_.setRetry(ms, count); }
    void onMessage(MessageHandler h, void *context = nullptr) { engine_.onMessage(h, context); }

    bool connect() { return engine_.connect(); }
    bool connect(const char *clientId);
    void disconnect() { engine_.disconnect(); }
    bool connected() const { return engine_.connected(); }
    int registerTopic(const char *name) { return engine_.registerTopic(name); }
    bool publish(const char *topic, const uint8_t *data, size_t len, uint8_t qos = 0, bool retain = false) { return engine_.publish(topic, data, len, qos, retain); }
    bool publish(const char *topic, const char *payload, uint8_t qos = 0, bool retain = false);
    int subscribe(const char *filter, uint8_t qos = 0) { return engine_.subscribe(filter, qos); }
    int subscribePredefined(uint16_t topicId, uint8_t qos = 0) { return engine_.subscribePredefined(topicId, qos); }
    bool unsubscribe(const char *filter) { return engine_.unsubscribe(filter); }
    void update() { engine_.update(); }
    uint8_t lastReturnCode() const { return engine_.lastReturnCode(); }
#endif

#if TINYMQ_SN_CLIENT_LEVEL >= 2
    void setWill(const char *topic, const char *payload, uint8_t qos = 0, bool retain = false);
    void clearWill() { engine_.clearWill(); }
    bool sleep(uint16_t seconds) { return engine_.sleep(seconds); }
    bool asleep() const { return engine_.asleep(); }
    bool checkMessages() { return engine_.checkMessages(); }
    // SEARCHGW goes to `broadcast`; the first gateway to answer is used
    int searchGateway(IPAddress broadcast, uint16_t port = 1884, uint8_t radius = 1, uint32_t timeoutMs = 5000);
#endif

    tinymq::SnClientEngine &engine() { return engine_; }

private:
    bool send(const uint8_t *data, size_t len) override;
    size_t receive(uint8_t *buf, size_t cap) override;
    uint32_t now() override { return millis(); }
    void idle() override;
    bool broadcast(const uint8_t *data, size_t len) override;
    void useLastSender() override;

    UDP &udp_;
    tinymq::SnClientEngine engine_;
    IPAddress gateway_;
    uint16_t port_ = 1884;
    IPAddress lastIp_;
    uint16_t lastPort_ = 0;
    IPAddress broadcast_;
    uint16_t broadcastPort_ = 1884;
    IdleHandler onIdle_ = nullptr;
    void *onIdleCtx_ = nullptr;
};
