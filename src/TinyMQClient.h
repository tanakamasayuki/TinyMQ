// MQTT client over any Arduino Client (WiFiClient, NetworkClient,
// EthernetClient, WiFiClientSecure for TLS, ...). Call update() from loop().
#pragma once

#include <Arduino.h>
#include <Client.h>
#include <IPAddress.h>

#include "tinymq/mq_client.h"

class TinyMQClient : private tinymq::Link {
public:
    typedef tinymq::ClientEngine::MessageHandler MessageHandler;
    typedef tinymq::ClientEngine::ConnackHandler ConnackHandler;
    typedef tinymq::ClientEngine::SubackHandler SubackHandler;
    typedef tinymq::ClientEngine::PublishedHandler PublishedHandler;
    typedef tinymq::ClientEngine::ClosedHandler ClosedHandler;

    explicit TinyMQClient(Client &client);

    void setServer(IPAddress ip, uint16_t port);
    void setServer(const char *host, uint16_t port);  // `host` is not copied

    // Settings used by the next connect(). Strings are not copied.
    void setProtocolVersion(uint8_t version) { engine_.setProtocolVersion(version); }  // tinymq::kMqtt311 (default) / kMqtt5 / kMqtt31
    void setClientId(const char *id) { engine_.setClientId(id); }
    void setKeepAlive(uint16_t seconds) { engine_.setKeepAlive(seconds); }
    void setCleanSession(bool clean) { engine_.setCleanSession(clean); }
    void setCredentials(const char *username, const char *password) { engine_.setCredentials(username, password); }
    void setWill(const char *topic, const char *payload, uint8_t qos = 0, bool retain = false);
    void setWill(const char *topic, const uint8_t *payload, size_t len, uint8_t qos = 0, bool retain = false) { engine_.setWill(topic, payload, len, qos, retain); }
    void clearWill() { engine_.clearWill(); }
    void setConnectTimeout(uint32_t ms);     // wait for CONNACK (default 10 s)
    // 5.0 (see tinymq::ClientEngine): session expiry, more CONNECT
    // properties, the will's properties (blocks built with tinymq::PropWriter)
    void setSessionExpiry(uint32_t seconds) { engine_.setSessionExpiry(seconds); }
    void setConnectProperties(tinymq::Bytes props) { engine_.setConnectProperties(props); }
    void setWillProperties(tinymq::Bytes props) { engine_.setWillProperties(props); }
    void setReconnectInterval(uint32_t ms);  // 0 (default): no automatic reconnect

    void onMessage(MessageHandler h, void *context = nullptr) { engine_.onMessage(h, context); }
    void onConnect(ConnackHandler h, void *context = nullptr) { engine_.onConnack(h, context); }
    void onSubscribe(SubackHandler h, void *context = nullptr) { engine_.onSuback(h, context); }
    void onPublished(PublishedHandler h, void *context = nullptr) { engine_.onPublished(h, context); }
    void onDisconnect(ClosedHandler h, void *context = nullptr) { engine_.onClosed(h, context); }

    // Opens the connection and waits for CONNACK. True when accepted.
    bool connect();
    bool connect(const char *clientId);
    // 5.0: tinymq::kDisconnectWithWill asks the server to publish the will
    void disconnect(uint8_t reason = tinymq::kSuccess);
    bool connected() const { return engine_.connected(); }
    void update();

    // Packet identifier for QoS 1 / 2, 0 for QoS 0, -1 on failure
    int publish(const char *topic, const uint8_t *payload, size_t len, uint8_t qos = 0, bool retain = false);
    int publish(const char *topic, const char *payload, uint8_t qos = 0, bool retain = false);
    // 5.0: with a PUBLISH property block
    int publish(const char *topic, const uint8_t *payload, size_t len, uint8_t qos, bool retain, tinymq::Bytes props) { return engine_.publish(topic, payload, len, qos, retain, props); }
    int subscribe(const char *filter, uint8_t qos = 0) { return engine_.subscribe(filter, qos); }
    // 5.0: subscription options and identifier
    int subscribe(const char *filter, tinymq::SubOptions opts, uint32_t subscriptionId = 0) { return engine_.subscribe(filter, opts, subscriptionId); }
    int unsubscribe(const char *filter) { return engine_.unsubscribe(filter); }

    // Why the last connection ended or was refused (a 5.0 reason code)
    uint8_t lastReason() const { return engine_.lastReason(); }
    size_t inflight() const { return engine_.inflight(); }
    uint32_t droppedMessages() const { return engine_.droppedMessages(); }
    const char *assignedClientId() const { return engine_.assignedClientId(); }  // 5.0, "" if none

    tinymq::ClientEngine &engine() { return engine_; }

private:
    size_t write(const uint8_t *data, size_t len) override;
    void close() override;
    bool openAndStart();
    void readAvailable();

    Client &client_;
    tinymq::ClientEngine engine_;
    IPAddress ip_;
    const char *host_ = nullptr;
    uint16_t port_ = 1883;
    uint32_t connectTimeout_ = 10000;
    uint32_t reconnectInterval_ = 0;
    uint32_t lastAttempt_ = 0;
    bool wanted_ = false;  // connect() was called and disconnect() was not
};
