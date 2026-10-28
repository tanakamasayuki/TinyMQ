// MQTT-SN 1.2 client engine, with no dependency on Arduino.
//
// Procedures (connect, register, subscribe, QoS 1 / 2 publish) block until
// the gateway answers, resending as MQTT-SN asks, so nothing is copied and
// kept: the RAM is the buffers and the topic table. Messages arriving
// meanwhile are delivered through the callback; do not start another
// blocking procedure from it (QoS -1 / 0 publish is fine).
//
// The features are chosen with TINYMQ_SN_CLIENT_LEVEL (mq_config.h).
#pragma once

#include "mq_config.h"
#include "mq_sn.h"

namespace tinymq {

// One datagram at a time to and from the gateway
class SnTransport {
public:
    virtual ~SnTransport() {}
    virtual bool send(const uint8_t *data, size_t len) = 0;
    // One datagram if there is one (from any sender), else 0
    virtual size_t receive(uint8_t *buf, size_t cap) = 0;
    virtual uint32_t now() = 0;
    virtual void idle() {}
    // For the gateway search: send to the broadcast address, and make the
    // sender of the last received datagram the gateway
    virtual bool broadcast(const uint8_t *data, size_t len)
    {
        (void)data;
        (void)len;
        return false;
    }
    virtual void useLastSender() {}
};

// A received message. `topic` is the name when known (a registered or short
// topic), else null; `topicId` and `topicType` are as sent.
struct SnMessage {
    const char *topic;
    uint16_t topicId;
    uint8_t topicType;
    const uint8_t *payload;
    size_t payloadLen;
    int8_t qos;
    bool retain;
    bool dup;
};

class SnClientEngine {
public:
    typedef void (*MessageHandler)(const SnMessage &msg, void *context);

    explicit SnClientEngine(SnTransport &transport) : t_(transport) {}

    // ---- level 0 and up: QoS -1 needs no connection (QoS 0 / 1 / 2 do)
    bool publishPredefined(uint16_t topicId, const uint8_t *data, size_t len, int8_t qos = -1, bool retain = false);
    bool publishShort(const char *topic, const uint8_t *data, size_t len, int8_t qos = -1, bool retain = false);

#if TINYMQ_SN_CLIENT_LEVEL >= 1
    void setClientId(const char *id) { clientId_ = id; }  // 1 to 23 characters, not copied
    void setKeepAlive(uint16_t seconds) { keepAlive_ = seconds; }
    void setCleanSession(bool clean) { clean_ = clean; }
    void setRetry(uint32_t ms, uint8_t count);  // Tretry (default 10 s) and Nretry (default 3)
    void onMessage(MessageHandler h, void *context = nullptr) { onMessage_ = h; onMessageCtx_ = context; }

    bool connect();
    void disconnect();
    bool connected() const { return state_ == kActive; }
    // Topic id of a registered name (registering it now if needed), or -1
    int registerTopic(const char *name);
    bool publish(const char *topic, const uint8_t *data, size_t len, uint8_t qos = 0, bool retain = false);
    bool publishId(uint16_t topicId, const uint8_t *data, size_t len, uint8_t qos = 0, bool retain = false);
    // Granted QoS, or -1
    int subscribe(const char *filter, uint8_t qos = 0);
    int subscribePredefined(uint16_t topicId, uint8_t qos = 0);
    bool unsubscribe(const char *filter);
    // Receive and answer, send keep alive. Call from loop().
    void update();
    uint8_t lastReturnCode() const { return lastRc_; }
#endif

#if TINYMQ_SN_CLIENT_LEVEL >= 2
    void setWill(const char *topic, const uint8_t *payload, size_t len, uint8_t qos = 0, bool retain = false);
    void clearWill() { willTopic_ = nullptr; }
    // Tell the gateway to keep messages for `seconds`, then stop talking.
    bool sleep(uint16_t seconds);
    bool asleep() const { return state_ == kAsleep; }
    // While asleep: collect what the gateway kept (delivered through the
    // callback). The gateway expects this within the sleep duration.
    bool checkMessages();
    // Broadcast SEARCHGW and take the first GWINFO or ADVERTISE sender as
    // the gateway. Returns its gateway id, or -1.
    int searchGateway(uint8_t radius = 1, uint32_t timeoutMs = 5000);
#endif

private:
    enum State : uint8_t { kDisconnected, kActive, kAsleep };

    bool sendTx(size_t n);
#if TINYMQ_SN_CLIENT_LEVEL >= 1
    // Sends req_[0..n) and waits for `want` (with the message id `msgId` when
    // nonzero), resending. The reply body is left in replyBody_/replyLen_.
    bool request(size_t n, uint8_t want, uint16_t msgId, bool setDupOnResend = false);
    bool pump();  // one datagram, if any; false if none
    void handle(uint8_t type, const uint8_t *body, size_t len);
    void deliver(const sn::Publish &p);
    uint16_t nextMsgId();
    int findTopic(const char *name, size_t len) const;
    int findTopic(uint16_t id) const;
    void remember(uint16_t id, const char *name, size_t len);
    bool publishWith(uint16_t topicId, uint8_t topicType, const uint8_t *data, size_t len, uint8_t qos, bool retain);
    void lost();
#endif

    SnTransport &t_;
    uint8_t tx_[TINYMQ_SN_CLIENT_BUFFER];
    State state_ = kDisconnected;

#if TINYMQ_SN_CLIENT_LEVEL >= 1
    uint8_t rx_[TINYMQ_SN_CLIENT_BUFFER];
    uint8_t req_[TINYMQ_SN_CLIENT_BUFFER];
    const uint8_t *replyBody_ = nullptr;
    size_t replyLen_ = 0;
    bool inRequest_ = false;

    const char *clientId_ = "tinymq-sn";
    uint16_t keepAlive_ = 60;
    bool clean_ = true;
    uint32_t retryMs_ = 10000;
    uint8_t retryCount_ = 3;
    uint16_t msgId_ = 0;
    uint8_t lastRc_ = 0;
    uint32_t lastTx_ = 0;
    uint32_t pingAt_ = 0;
    uint8_t pingTries_ = 0;
    bool pingOutstanding_ = false;

    struct Topic {
        uint16_t id;  // 0: free
        char name[TINYMQ_SN_CLIENT_TOPIC_LEN + 1];
    };
    Topic topics_[TINYMQ_SN_CLIENT_MAX_TOPICS];
    uint8_t topicNext_ = 0;

    MessageHandler onMessage_ = nullptr;
    void *onMessageCtx_ = nullptr;
#endif

#if TINYMQ_SN_CLIENT_LEVEL >= 2
    uint16_t qos2In_[4] = {0, 0, 0, 0};  // received QoS 2 message ids awaiting PUBREL
    const char *willTopic_ = nullptr;
    const uint8_t *willPayload_ = nullptr;
    size_t willLen_ = 0;
    uint8_t willQos_ = 0;
    bool willRetain_ = false;
    uint16_t sleepSeconds_ = 0;
#endif
};

}  // namespace tinymq
