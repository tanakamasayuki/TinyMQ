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

    void onMessage(MessageHandler h, void *context = nullptr) { onMessage_ = h; onMessageCtx_ = context; }
    void onConnack(ConnackHandler h, void *context = nullptr) { onConnack_ = h; onConnackCtx_ = context; }
    void onSuback(SubackHandler h, void *context = nullptr) { onSuback_ = h; onSubackCtx_ = context; }
    void onPublished(PublishedHandler h, void *context = nullptr) { onPublished_ = h; onPublishedCtx_ = context; }
    void onClosed(ClosedHandler h, void *context = nullptr) { onClosed_ = h; onClosedCtx_ = context; }

    // ---- transport side
    // The stream is open: send CONNECT. False if it could not be sent.
    bool start(Link &link, uint32_t now);
    uint8_t *rxBuffer(size_t &room);
    void rxCommit(size_t n, uint32_t now);
    void poll(uint32_t now);
    // The transport saw the stream close.
    void linkLost();

    // ---- application side
    State state() const { return state_; }
    bool connected() const { return state_ == State::Connected; }
    // Packet identifier for QoS 1 / 2, 0 for QoS 0, -1 on failure
    int publish(const char *topic, const uint8_t *payload, size_t len, uint8_t qos, bool retain);
    int subscribe(const char *filter, uint8_t qos);
    int unsubscribe(const char *filter);
    void disconnect();

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
    void clearSession();
    uint16_t nextPacketId();
    bool packetIdInUse(uint16_t id) const;
    void handlePacket(const FixedHeader &h, const uint8_t *body);
    void handleConnack(const uint8_t *body, size_t len);
    void handlePublish(const FixedHeader &h, const uint8_t *body, size_t len);
    void handleAck(const FixedHeader &h, const uint8_t *body, size_t len);
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
    uint32_t now_ = 0;
    uint32_t lastTx_ = 0;
    uint32_t pingSentAt_ = 0;
    uint32_t connectSentAt_ = 0;
    bool pingOutstanding_ = false;
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
};

}  // namespace tinymq
