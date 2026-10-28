// MQTT broker. Give it one or more listeners (TinyMQTcpListener for TCP)
// and call update() from loop().
#pragma once

#include <Arduino.h>

#include "tinymq/mq_broker.h"

// A source of client connections for TinyMQBroker
class TinyMQListener {
public:
    virtual ~TinyMQListener() {}
    virtual void update(tinymq::Broker &engine, uint32_t now) = 0;
};

#ifndef TINYMQ_BROKER_MAX_LISTENERS
#define TINYMQ_BROKER_MAX_LISTENERS 4
#endif

class TinyMQBroker {
public:
    typedef tinymq::Broker::AuthHandler AuthHandler;
    typedef tinymq::Broker::AclHandler AclHandler;
    typedef tinymq::Broker::MessageHandler MessageHandler;
    typedef tinymq::Broker::ClientHandler ClientHandler;

    // `poolBytes` is used only when TINYMQ_BROKER_POOL_BYTES is 0
    bool begin(size_t poolBytes = 0) { return engine_.begin(poolBytes); }
    bool addListener(TinyMQListener &listener);
    void end() { engine_.end(); }  // closes every connection and forgets everything
    void update();

    void setMaxQos(uint8_t qos) { engine_.setMaxQos(qos); }
    void setConnectTimeout(uint32_t ms) { engine_.setConnectTimeout(ms); }
    void setRetainAvailable(bool on) { engine_.setRetainAvailable(on); }
    void setSessionExpiry(uint32_t seconds) { engine_.setSessionExpiry(seconds); }
    void setSysInterval(uint32_t seconds) { engine_.setSysInterval(seconds); }  // $SYS/broker/... every `seconds` (0: off)
    void onAuthenticate(AuthHandler h, void *context = nullptr) { engine_.onAuthenticate(h, context); }
    void onAuthorize(AclHandler h, void *context = nullptr) { engine_.onAuthorize(h, context); }
    void onClient(ClientHandler h, void *context = nullptr) { engine_.onClient(h, context); }

    // The sketch as a client of its own broker
    bool publish(const char *topic, const uint8_t *payload, size_t len, uint8_t qos = 0, bool retain = false) { return engine_.publish(topic, payload, len, qos, retain); }
    bool publish(const char *topic, const char *payload, uint8_t qos = 0, bool retain = false);
    bool subscribe(const char *filter, uint8_t qos = 0) { return engine_.subscribe(filter, qos); }
    bool unsubscribe(const char *filter) { return engine_.unsubscribe(filter); }
    void onMessage(MessageHandler h, void *context = nullptr) { engine_.onMessage(h, context); }

    // Persistence: retained messages and persistent sessions to and from any
    // storage (see tinymq::StateSink / StateSource)
    bool saveState(tinymq::StateSink &sink) { return engine_.saveState(sink); }
    bool loadState(tinymq::StateSource &source) { return engine_.loadState(source); }
    void onRetainedChange(tinymq::Broker::RetainedHandler h, void *context = nullptr) { engine_.onRetainedChange(h, context); }

    size_t clients() const { return engine_.clients(); }
    size_t sessions() const { return engine_.sessions(); }
    size_t subscriptions() const { return engine_.subscriptions(); }
    size_t retained() const { return engine_.retained(); }
    size_t poolUsed() const { return engine_.poolUsed(); }
    size_t poolCapacity() const { return engine_.poolCapacity(); }
    uint32_t droppedMessages() const { return engine_.droppedMessages(); }

    tinymq::Broker &engine() { return engine_; }

private:
    tinymq::Broker engine_;
    TinyMQListener *listeners_[TINYMQ_BROKER_MAX_LISTENERS];
    size_t listenerCount_ = 0;
};
