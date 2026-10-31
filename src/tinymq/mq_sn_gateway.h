// MQTT-SN 1.2 gateway built into the broker, with no dependency on Arduino
// or lwIP.
//
// Each MQTT-SN client takes one broker connection: its MQTT-SN messages are
// rebuilt as MQTT packets and fed to the broker, and the broker's packets to
// it are turned into MQTT-SN datagrams. Sessions, QoS, retained messages,
// wills and authorisation are the broker's.
//
// The transport passes each datagram to input() and calls poll() regularly;
// the gateway sends through a DatagramSender. TinyMQSnListener does this
// over an Arduino UDP.
#pragma once

#include "mq_broker.h"
#include "mq_sn.h"

namespace tinymq {

// A client's address: IPv4 and UDP port, plus the Wireless Node Id when it
// is behind a forwarder (Forwarder Encapsulation; nodeLen 0 otherwise)
struct Endpoint {
    uint8_t ip[4];
    uint16_t port;
    uint8_t nodeLen = 0;
    uint8_t node[TINYMQ_SN_MAX_NODE_ID];

    bool operator==(const Endpoint &o) const
    {
        return port == o.port && memcmp(ip, o.ip, 4) == 0 && nodeLen == o.nodeLen && memcmp(node, o.node, nodeLen) == 0;
    }
    bool operator!=(const Endpoint &o) const { return !(*this == o); }
};

class DatagramSender {
public:
    virtual ~DatagramSender() {}
    virtual void sendTo(const Endpoint &to, const uint8_t *data, size_t len) = 0;
};

class SnGateway {
public:
    // Whether datagrams from `from` are taken (after the forwarder's
    // encapsulation is removed, so the node id is known)
    typedef bool (*SourceFilter)(const Endpoint &from, void *context);

    SnGateway();

    void begin(Broker &broker, DatagramSender &out);
    bool attached() const { return broker_ != nullptr; }

    // ---- settings
    // `name` is not copied. Ids are shared by all clients.
    bool addPredefinedTopic(uint16_t id, const char *name);
    void setGatewayId(uint8_t id) { gwId_ = id; }
    // ADVERTISE every `seconds` to `to` (a broadcast address); 0 turns it off
    void setAdvertise(uint16_t seconds, const Endpoint &to);
    // Unacknowledged messages to a client are resent every `ms`; after
    // `count` resends the client is taken as lost (its will is published).
    void setRetry(uint32_t ms, uint8_t count);
    // Accept PUBLISH with QoS -1 from any address (default true)
    void setQosMinusOne(bool on) { qosMinusOne_ = on; }
    // Datagrams from senders the filter refuses are dropped (default: all taken)
    void setSourceFilter(SourceFilter f, void *context = nullptr) { filter_ = f; filterCtx_ = context; }

    // ---- transport side
    void input(const Endpoint &from, const uint8_t *data, size_t len, uint32_t now);
    void poll(uint32_t now);

    size_t clients() const;
    size_t registrations() const;

private:
    enum State : uint8_t { kFree, kWaitWillTopic, kWaitWillMsg, kWaitConnack, kActive, kAsleep, kAwake };

    struct Client : Link {
        SnGateway *gw = nullptr;
        uint8_t index = 0;
        uint8_t state = kFree;
        Endpoint addr;
        char clientId[24];
        uint8_t clientIdLen = 0;
        bool clean = true;
        int conn = -1;
        uint16_t duration = 0;  // keep alive or sleep, seconds
        uint32_t lastRx = 0;
        uint32_t connectedAt = 0;
        uint32_t wokeAt = 0;
        uint32_t lastProgress = 0;  // last send or acknowledgement, for resending
        uint8_t retries = 0;
        uint16_t nextTopicId = 1;
        uint16_t gwMsgId = 0;
        // will being collected during CONNECT
        uint32_t willTopic = Pool::kNull;
        uint16_t willTopicLen = 0;
        uint8_t willQos = 0;
        bool willRetain = false;
        // topic id of the client's recent QoS 1 / 2 publishes and subscriptions,
        // by message id, for PUBACK and SUBACK
        uint16_t ackMsgId[8];
        uint16_t ackTopicId[8];
        uint8_t ackNext = 0;

        size_t write(const uint8_t *data, size_t len) override;
        void close() override;
        size_t writable() override;
    };

    struct Registration {
        uint8_t client;  // 0xFF: free
        uint16_t topicId;
        uint32_t name;   // pool handle
        uint16_t nameLen;
    };

    struct Predefined {
        uint16_t id;
        const char *name;
    };

    // An acknowledgement the gateway gives the broker in a client's place
    // (a message it could not deliver), fed once the broker is not in a call
    struct Deferred {
        uint8_t client = 0xFF;
        PacketType type;
        uint16_t msgId;
    };

    // datagrams
    void send(const Endpoint &to, size_t n);
    void sendTo(Client &c, size_t n) { send(c.addr, n); }

    // clients
    Client *findByAddr(const Endpoint &a);
    Client *findById(Bytes id);
    Client *allocClient();
    void freeClient(Client &c);
    void startSession(Client &c, Bytes willPayload);
    void lost(Client &c);
    void feedDisconnect(Client &c);
    void finishWake(Client &c, bool force);

    // topics
    Bytes regName(const Registration &r) const { return Bytes(pool_.ptr(r.name), r.nameLen); }
    int findRegistration(uint8_t client, Bytes name) const;
    int findRegistration(uint8_t client, uint16_t topicId) const;
    int registerTopic(Client &c, Bytes name);  // index or -1
    bool topicName(Client &c, uint8_t type, uint16_t id, Bytes &name, char shortBuf[2]) const;
    const Predefined *predefinedByName(Bytes name) const;
    const Predefined *predefinedById(uint16_t id) const;
    void rememberAck(Client &c, uint16_t msgId, uint16_t topicId);
    uint16_t recallAck(Client &c, uint16_t msgId) const;

    // MQTT-SN messages from a client
    void onConnect(const Endpoint &from, const uint8_t *body, size_t len, uint32_t now);
    void onPublish(Client *c, const Endpoint &from, const uint8_t *body, size_t len);
    void onRegister(Client &c, const uint8_t *body, size_t len);
    void onRegack(Client &c, const uint8_t *body, size_t len);
    void onSubscribe(Client &c, uint8_t type, const uint8_t *body, size_t len);
    void onPingreq(Client *c, const Endpoint &from, const uint8_t *body, size_t len, uint32_t now);
    void onDisconnect(Client &c, const uint8_t *body, size_t len);
    void onWillUpdate(Client &c, uint8_t type, const uint8_t *body, size_t len);
    void feedAck(Client &c, PacketType type, uint16_t msgId);
    void defer(Client &c, PacketType type, uint16_t msgId);
    void processDeferred();

    // MQTT packets from the broker to a client
    size_t clientWrite(Client &c, const uint8_t *data, size_t len);
    void clientClosed(Client &c);
    void translate(Client &c, const FixedHeader &h, const uint8_t *body);
    void sendPublish(Client &c, const Publish &p, uint16_t topicId, uint8_t topicType);
    void releaseHeld(Client &c, bool registered);

    // broker input: an MQTT packet built in the connection's receive buffer
    template <typename F>
    void feed(Client &c, F encode);

    Broker *broker_ = nullptr;
    DatagramSender *out_ = nullptr;
    uint32_t now_ = 0;

    Client clients_[TINYMQ_SN_MAX_CLIENTS];
    Registration regs_[TINYMQ_SN_MAX_TOPICS];
    Predefined predefined_[TINYMQ_SN_MAX_PREDEFINED];
    Deferred deferred_[8];
    size_t predefinedCount_ = 0;

    // outgoing datagram, with room for a forwarder's encapsulation in front
    uint8_t tx_[TINYMQ_MAX_PACKET + 8 + 3 + TINYMQ_SN_MAX_NODE_ID];
    uint8_t asm_[TINYMQ_MAX_PACKET + 8];     // an MQTT packet from the broker being assembled
    size_t asmLen_ = 0;
    int asmOwner_ = -1;
    uint8_t held_[TINYMQ_MAX_PACKET + 8];    // a PUBLISH waiting for REGACK
    size_t heldLen_ = 0;
    int heldOwner_ = -1;
    uint16_t heldRegMsgId_ = 0;
    int heldReg_ = -1;

    alignas(4) uint8_t arena_[TINYMQ_SN_POOL_BYTES];
    Pool pool_;

    uint8_t gwId_ = 1;
    uint16_t advertiseSeconds_ = 0;
    Endpoint advertiseTo_;
    uint32_t lastAdvertise_ = 0;
    uint32_t retryMs_ = 10000;
    uint8_t retryCount_ = 3;
    bool qosMinusOne_ = true;
    SourceFilter filter_ = nullptr;
    void *filterCtx_ = nullptr;
};

}  // namespace tinymq
