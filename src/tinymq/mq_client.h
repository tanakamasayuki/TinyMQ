// MQTT client engine: the session over one byte stream, with no
// dependency on Arduino or lwIP.
//
// The transport opens the stream, calls start(), feeds received bytes with
// rxBuffer() / rxCommit(), and calls poll() regularly; the engine writes
// through a Link. TinyMQClient does this over an Arduino Client.
#pragma once

#include "mq_codec.h"
#include "mq_config.h"
#include "mq_pool.h"
#include "mq_props.h"

namespace tinymq {

class ClientEngine {
public:
    enum class State : uint8_t { Disconnected, WaitConnack, Connected };

    typedef void (*MessageHandler)(const Message &msg, void *context);
    typedef void (*ConnackHandler)(uint8_t reason, bool sessionPresent, void *context);
    // SUBACK / UNSUBACK: one reason per filter (the granted QoS 0-2, or >= 0x80)
    typedef void (*SubackHandler)(uint16_t packetId, bool unsubscribe, const uint8_t *reasons, size_t count, void *context);
    // A QoS 1 / 2 publish completed (PUBACK / PUBCOMP)
    typedef void (*PublishedHandler)(uint16_t packetId, uint8_t reason, void *context);
    // The stream was closed, by either side. `reason` says why (kSuccess for disconnect())
    typedef void (*ClosedHandler)(uint8_t reason, void *context);
    // 5.0 enhanced authentication: the method (and the first data) go in
    // CONNECT with setConnectProperties(). Called with kContinueAuthentication
    // for each AUTH asking for an answer (write it to `reply`, at most `cap`
    // = TINYMQ_AUTH_DATA bytes, and set `replyLen`), and with kSuccess at the
    // end (CONNACK, or the AUTH that ends re-authentication; `reply` unused).
    // Return false to give up: the connection ends.
    typedef bool (*AuthHandler)(uint8_t reason, Bytes method, Bytes data, uint8_t *reply, size_t cap, size_t &replyLen, void *context);

    ClientEngine();

    // ---- settings (strings are not copied: keep them valid while connected)
    void setProtocolVersion(uint8_t version) { connect_.version = version; }
    uint8_t protocolVersion() const { return connect_.version; }
    void setClientId(const char *id) { connect_.clientId = Bytes(id); }
    void setKeepAlive(uint16_t seconds) { connect_.keepAlive = seconds; }
    void setCleanSession(bool clean) { connect_.cleanStart = clean; }
    void setCredentials(const char *username, const char *password);
    void setWill(const char *topic, const uint8_t *payload, size_t len, uint8_t qos, bool retain);
    void clearWill() { connect_.hasWill = false; }
    void setConnackTimeout(uint32_t ms) { connackTimeout_ = ms; }
    // 5.0: how long the server keeps the session after the connection ends
    void setSessionExpiry(uint32_t seconds) { sessionExpiry_ = seconds; }
    // 5.0: more CONNECT properties (user properties, ...) and the will's
    // properties (Will Delay Interval, Message Expiry Interval, ...), as
    // property blocks built with PropWriter. Not copied.
    void setConnectProperties(Bytes props) { extraConnectProps_ = props; }
    void setWillProperties(Bytes props) { connect_.willProps = props; }

    void onMessage(MessageHandler h, void *context = nullptr) { onMessage_ = h; onMessageCtx_ = context; }
    void onConnack(ConnackHandler h, void *context = nullptr) { onConnack_ = h; onConnackCtx_ = context; }
    void onSuback(SubackHandler h, void *context = nullptr) { onSuback_ = h; onSubackCtx_ = context; }
    void onPublished(PublishedHandler h, void *context = nullptr) { onPublished_ = h; onPublishedCtx_ = context; }
    void onClosed(ClosedHandler h, void *context = nullptr) { onClosed_ = h; onClosedCtx_ = context; }
    void onAuth(AuthHandler h, void *context = nullptr) { onAuth_ = h; onAuthCtx_ = context; }

    // ---- transport side
    // The stream is open: send CONNECT. False if it could not be sent.
    bool start(Link &link, uint32_t now);
    uint8_t *rxBuffer(size_t &room);
    void rxCommit(size_t n, uint32_t now);
    void poll(uint32_t now);
    // The transport saw the stream close.
    void linkLost();
    // The transport could not open the stream at all.
    void linkFailed() { lastReason_ = kUnspecifiedError; }

    // ---- application side
    State state() const { return state_; }
    bool connected() const { return state_ == State::Connected; }
    // Packet identifier for QoS 1 / 2, 0 for QoS 0, -1 on failure. `props`
    // (5.0) is a PUBLISH property block; it may not hold a Topic Alias or a
    // Subscription Identifier.
    int publish(const char *topic, const uint8_t *payload, size_t len, uint8_t qos, bool retain, Bytes props = Bytes());
    int subscribe(const char *filter, uint8_t qos);
    // 5.0 options (No Local, Retain As Published, Retain Handling) and a
    // subscription identifier (0: none)
    int subscribe(const char *filter, SubOptions opts, uint32_t subscriptionId = 0);
    int unsubscribe(const char *filter);
    // 5.0: `reason` kDisconnectWithWill asks the server to publish the will
    void disconnect(uint8_t reason = kSuccess);
    // 5.0: re-authentication with the Authentication Method of CONNECT
    // (AUTH 0x19 carrying `data`). False if not connected with one.
    bool reauthenticate(Bytes data = Bytes());

    // ---- what the server said in CONNACK (5.0)
    const char *assignedClientId() const { return assigned_; }  // "" if none
    uint16_t serverReceiveMaximum() const { return serverReceiveMax_; }
    uint32_t serverMaximumPacketSize() const { return serverMaxPacket_; }
    uint8_t serverMaximumQos() const { return serverMaxQos_; }
    bool serverRetainAvailable() const { return serverRetain_; }
    uint16_t keepAlive() const { return keepAlive_; }  // the client's, or the server's Server Keep Alive

    uint8_t lastReason() const { return lastReason_; }
    size_t inflight() const { return inflightCount_; }
    uint32_t droppedMessages() const { return dropped_; }  // too large for the receive buffer

private:
    enum InflightState : uint8_t { WaitPuback, WaitPubrec, WaitPubcomp };
    struct Inflight {
        uint16_t packetId;
        uint8_t state;
        uint32_t msg;  // pool handle, kNull once PUBREC arrived
    };

    bool send(const uint8_t *data, size_t len);
    bool sendPublish(uint32_t msg, uint16_t packetId, bool dup);
    bool sendAck(PacketType type, uint16_t packetId);
    void close(uint8_t reason);
    // A protocol error found here: a 5.0 server hears `reason` first
    void fail(uint8_t reason);
    void clearSession();
    uint16_t nextPacketId();
    bool packetIdInUse(uint16_t id) const;
    void handlePacket(const FixedHeader &h, const uint8_t *body);
    void handleConnack(const uint8_t *body, size_t len);
    void handlePublish(const FixedHeader &h, const uint8_t *body, size_t len);
    void handleAck(const FixedHeader &h, const uint8_t *body, size_t len);
    void handleAuth(const FixedHeader &h, const uint8_t *body, size_t len);
    // The Authentication Method of CONNECT; false if none
    bool authMethod(Bytes &method) const;
    bool sendAuth(uint8_t reason, Bytes data);
    void handleOversizePublish(const FixedHeader &h, const uint8_t *prefix, size_t len);
    int findInflight(uint16_t id, uint8_t state) const;
    void removeInflight(int index);
    bool rememberQos2(uint16_t id);
    bool forgetQos2(uint16_t id);
    bool knowsQos2(uint16_t id) const;

    Link *link_ = nullptr;
    State state_ = State::Disconnected;
    Connect connect_;
    uint32_t connackTimeout_ = 10000;
    uint32_t sessionExpiry_ = 0;
    Bytes extraConnectProps_;
    uint8_t connectProps_[TINYMQ_MAX_TOPIC_LEN + 64];
    uint16_t keepAlive_ = 0;
    char assigned_[TINYMQ_MAX_TOPIC_LEN + 1] = {0};
    uint16_t serverReceiveMax_ = 0xFFFF;
    uint32_t serverMaxPacket_ = 0xFFFFFFFF;
    uint8_t serverMaxQos_ = 2;
    bool serverRetain_ = true;
    uint32_t now_ = 0;
    uint32_t lastTx_ = 0;
    uint32_t pingSentAt_ = 0;
    uint32_t connectSentAt_ = 0;
    bool pingOutstanding_ = false;
    bool reauth_ = false;
    uint8_t lastReason_ = kSuccess;
    uint16_t lastPacketId_ = 0;
    uint32_t dropped_ = 0;

    uint8_t rx_[TINYMQ_MAX_PACKET];
    size_t rxLen_ = 0;
    uint32_t skip_ = 0;  // bytes of an oversize packet still to discard
    uint8_t tx_[TINYMQ_MAX_PACKET];
    char topic_[TINYMQ_MAX_TOPIC_LEN + 1];

    alignas(4) uint8_t arena_[TINYMQ_CLIENT_POOL_BYTES];
    Pool pool_;
    MessageStore store_;
    Inflight inflight_[TINYMQ_CLIENT_MAX_INFLIGHT];
    size_t inflightCount_ = 0;
    uint16_t qos2In_[TINYMQ_CLIENT_MAX_QOS2_IN];
    size_t qos2InCount_ = 0;

    MessageHandler onMessage_ = nullptr;
    void *onMessageCtx_ = nullptr;
    ConnackHandler onConnack_ = nullptr;
    void *onConnackCtx_ = nullptr;
    SubackHandler onSuback_ = nullptr;
    void *onSubackCtx_ = nullptr;
    PublishedHandler onPublished_ = nullptr;
    void *onPublishedCtx_ = nullptr;
    ClosedHandler onClosed_ = nullptr;
    void *onClosedCtx_ = nullptr;
    AuthHandler onAuth_ = nullptr;
    void *onAuthCtx_ = nullptr;
};

}  // namespace tinymq
