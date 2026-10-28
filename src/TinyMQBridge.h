// Bridge from TinyMQBroker to an upstream broker, like mosquitto's bridge.
// Messages are forwarded by a topic map: out (local to upstream), in
// (upstream to local) or both, with prefixes replaced on the way.
//
//   WiFiClient upstreamNet;            // or WiFiClientSecure for TLS
//   TinyMQBridge bridge(upstreamNet);
//   ...
//   bridge.client().setServer("broker.example.com", 1883);
//   bridge.client().setClientId("edge-1");
//   bridge.addTopic("sensors/#", TinyMQBridge::kOut, 1, "", "site1/");
//   bridge.addTopic("cmd/#", TinyMQBridge::kIn, 1, "", "site1/");
//   broker.addListener(bridge);        // update() runs from broker.update()
//
// Loops: the bridge never gets back from the local broker what it put in.
// Upstream, a 5.0 connection subscribes with No Local; on 3.1.1 give the
// two sides different prefixes for topics forwarded both ways.
#pragma once

#include "TinyMQBroker.h"
#include "TinyMQClient.h"

class TinyMQBridge : public TinyMQListener {
public:
    enum Direction : uint8_t { kOut = 1, kIn = 2, kBoth = 3 };

    explicit TinyMQBridge(Client &upstream);

    // The upstream connection: server, client id, credentials, version, will...
    TinyMQClient &client() { return client_; }
    void setReconnectInterval(uint32_t ms) { reconnectMs_ = ms; }  // default 5 s

    // `pattern` is a topic filter; the local side is localPrefix + pattern and
    // the upstream side remotePrefix + pattern. Strings are not copied.
    bool addTopic(const char *pattern, Direction direction, uint8_t qos = 0, const char *localPrefix = "", const char *remotePrefix = "");

    bool connected() const { return client_.connected(); }
    uint32_t forwardedOut() const { return out_; }
    uint32_t forwardedIn() const { return in_; }
    uint32_t dropped() const { return dropped_; }  // QoS 0 out while upstream was down, the queue full, or a topic too long
    size_t queued() const { return queued_; }       // QoS 1 / 2 kept for upstream

    void update(tinymq::Broker &engine, uint32_t now) override;

private:
    struct Topic {
        const char *pattern;
        const char *localPrefix;
        const char *remotePrefix;
        uint8_t direction;
        uint8_t qos;
    };

    static void onLocal(const tinymq::Message &m, void *self);
    static void onRemote(const tinymq::Message &m, void *self);
    static void onConnack(uint8_t reason, bool sessionPresent, void *self);
    void subscribeAll();
    // prefix + pattern into buf; false if too long
    static bool join(char *buf, const char *prefix, const char *pattern);
    // Topic `topic` under `from` prefix, re-rooted under `to`; false if too long
    static bool rewrite(char *buf, const char *topic, const char *from, const char *to);
    void keep(const tinymq::Message &m, uint8_t qos);  // for when upstream is back
    void sendKept();

    TinyMQClient client_;
    tinymq::Broker *engine_ = nullptr;
    int tap_ = -1;
    Topic topics_[TINYMQ_BRIDGE_MAX_TOPICS];
    size_t topicCount_ = 0;
    uint32_t reconnectMs_ = 5000;
    bool started_ = false;
    uint32_t out_ = 0;
    uint32_t in_ = 0;
    uint32_t dropped_ = 0;
    char filter_[TINYMQ_MAX_TOPIC_LEN + 1];
    char topic_[TINYMQ_MAX_TOPIC_LEN + 1];
#if TINYMQ_BRIDGE_QUEUE_BYTES > 0
    alignas(4) uint8_t arena_[TINYMQ_BRIDGE_QUEUE_BYTES];
    tinymq::Pool pool_;
    tinymq::MessageStore store_{pool_};
    uint32_t queue_[TINYMQ_BRIDGE_MAX_QUEUED];
    size_t queueHead_ = 0;
#endif
    size_t queued_ = 0;
};
