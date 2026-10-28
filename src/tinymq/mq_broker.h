// MQTT broker engine: connections, sessions, subscriptions, retained
// messages and delivery, with no dependency on Arduino or lwIP.
//
// A listener hands each new connection to accept(), feeds its bytes with
// rxBuffer() / rxCommit(), reports a closed stream with linkLost(), and
// poll() runs the timers. The engine writes through each connection's Link
// and closes it with Link::close(). TinyMQBroker does this with the
// listeners of TinyMQListener.h.
#pragma once

#include "mq_codec.h"
#include "mq_config.h"
#include "mq_pool.h"

namespace tinymq {

// What a client presented in CONNECT, for the authentication callback.
struct ConnectInfo {
    Bytes clientId;
    bool hasUsername;
    Bytes username;
    bool hasPassword;
    Bytes password;
    uint8_t version;
};

class Broker {
public:
    // Returns kSuccess to accept, or a reason code to refuse
    // (kBadUserNameOrPassword, kNotAuthorized, kClientIdentifierNotValid, ...).
    typedef uint8_t (*AuthHandler)(const ConnectInfo &info, void *context);
    // Whether the client may publish to / subscribe with `topic`.
    typedef bool (*AclHandler)(Bytes clientId, Bytes username, Bytes topic, bool subscribe, void *context);
    // A message for the local client's subscriptions.
    typedef void (*MessageHandler)(const Message &msg, void *context);
    // A client connected (true) or its connection ended (false).
    typedef void (*ClientHandler)(Bytes clientId, bool connected, void *context);

    static const uint16_t kNone = 0xFFFF;

    Broker();
    ~Broker();

    // Prepares the message pool: the static one, or `poolBytes` allocated
    // here once when TINYMQ_BROKER_POOL_BYTES is 0. False if that fails.
    bool begin(size_t poolBytes = 0);

    // ---- settings
    void setMaxQos(uint8_t qos);                         // highest QoS granted to subscribers (default 2)
    void setConnectTimeout(uint32_t ms) { connectTimeout_ = ms; }  // from accept() to CONNECT (default 10 s)
    void setRetainAvailable(bool on) { retainAvailable_ = on; }    // false: retained messages are not kept
    void setSessionExpiry(uint32_t seconds) { sessionExpiry_ = seconds; }  // offline sessions; 0 (default): kept until evicted
    void onAuthenticate(AuthHandler h, void *context = nullptr) { onAuth_ = h; onAuthCtx_ = context; }
    void onAuthorize(AclHandler h, void *context = nullptr) { onAcl_ = h; onAclCtx_ = context; }
    void onClient(ClientHandler h, void *context = nullptr) { onClient_ = h; onClientCtx_ = context; }

    // ---- listener side
    // A new stream: returns the connection number, or -1 when all are in use
    // (the caller closes the stream).
    int accept(Link &link, uint32_t now);
    uint8_t *rxBuffer(int conn, size_t &room);
    void rxCommit(int conn, size_t n, uint32_t now);
    void linkLost(int conn);
    bool isOpen(int conn) const { return conn >= 0 && conn < TINYMQ_BROKER_MAX_CLIENTS && conns_[conn].state != kFree; }
    // Whether `conn` is still the connection accepted for `link` (numbers are reused)
    bool owns(int conn, const Link &link) const { return isOpen(conn) && conns_[conn].link == &link; }
    void poll(uint32_t now);

    // For gateways that translate another protocol (MQTT-SN) per connection:
    void flushConn(int conn);        // send what is queued, as far as writable() allows
    void resendConn(int conn);       // resend the unacknowledged deliveries (with DUP) and PUBRELs
    bool hasInflight(int conn) const;
    bool hasQueued(int conn) const;  // waiting to be sent
    // The will of an open connection: replace or remove it. False if the pool is full.
    bool setWill(int conn, Bytes topic, Bytes payload, uint8_t qos, bool retain);
    void clearWill(int conn);
    // The current will; false if there is none
    bool will(int conn, Bytes &topic, Bytes &payload, uint8_t &qos, bool &retain) const;

    // ---- local client (the sketch, inside the broker)
    bool publish(const char *topic, const uint8_t *payload, size_t len, uint8_t qos = 0, bool retain = false);
    bool publish(Bytes topic, Bytes payload, uint8_t qos = 0, bool retain = false);
    bool subscribe(const char *filter, uint8_t qos = 0);  // retained messages are delivered at once
    bool unsubscribe(const char *filter);
    void onMessage(MessageHandler h, void *context = nullptr) { onLocal_ = h; onLocalCtx_ = context; }

    // ---- statistics
    size_t clients() const;        // connected
    size_t sessions() const;       // connected and offline persistent
    size_t subscriptions() const;
    size_t retained() const;
    size_t poolUsed() const { return pool_.used(); }
    size_t poolCapacity() const { return pool_.capacity(); }
    uint32_t droppedMessages() const { return dropped_; }  // queue full, pool full or no room for a retained message

private:
    enum ConnState : uint8_t { kFree, kWaitConnect, kActive };
    enum EntryState : uint8_t { kQueued, kWaitPuback, kWaitPubrec, kWaitPubcomp };
    static const uint16_t kLocal = 0xFFFE;

    struct Conn {
        Link *link;
        uint8_t state;
        uint8_t version;
        uint16_t session;
        uint16_t keepAlive;
        uint32_t lastRx;
        uint32_t openedAt;
        size_t rxLen;
        bool broken;  // a write failed; closed by reapBroken()
    };

    struct Entry {
        uint32_t msg;       // kNull once PUBREC arrived
        uint16_t packetId;  // 0 while queued
        uint8_t qos;
        uint8_t state;
        bool retain;        // the retain flag to send
    };

    struct Session {
        bool used;
        bool clean;
        uint16_t conn;
        uint32_t clientId;  // pool handles of the strings
        uint16_t clientIdLen;
        uint32_t username;
        uint16_t usernameLen;
        uint32_t will;      // message, kNull if none
        uint32_t lastSeen;
        uint16_t lastPacketId;
        uint8_t queued;
        uint8_t inflight;
        uint8_t qos2InCount;
        Entry queue[TINYMQ_BROKER_MAX_QUEUED];
        uint16_t qos2In[TINYMQ_BROKER_MAX_QOS2_IN];
    };

    struct Sub {
        uint16_t session;  // kNone when free
        uint8_t qos;
        uint8_t options;
        uint32_t filter;   // pool handle
        uint16_t filterLen;
    };

    // strings in the pool
    uint32_t storeString(Bytes s);
    Bytes string(uint32_t h, uint16_t len) const { return h == Pool::kNull ? Bytes() : Bytes(pool_.ptr(h), len); }

    // connections
    void handlePacket(int conn, const FixedHeader &h, const uint8_t *body);
    void handleConnect(int conn, const uint8_t *body, size_t len);
    void handlePublish(int conn, const FixedHeader &h, const uint8_t *body, size_t len);
    void handleAck(int conn, const FixedHeader &h, const uint8_t *body, size_t len);
    void handleSubscribe(int conn, const uint8_t *body, size_t len);
    void handleUnsubscribe(int conn, const uint8_t *body, size_t len);
    void closeConn(int conn, bool publishWill);
    void reapBroken();
    bool send(int conn, const uint8_t *data, size_t len);
    bool sendAck(int conn, PacketType type, uint16_t packetId);
    bool sendConnack(int conn, uint8_t version, uint8_t reason, bool sessionPresent);
    bool sendPublish(int conn, uint32_t msg, uint8_t qos, uint16_t packetId, bool retain, bool dup);

    // sessions
    int findSession(Bytes clientId) const;
    int newSession(uint32_t now);
    void destroySession(int s);
    void clearSessionState(int s);
    void detachSession(int s, uint32_t now);
    uint16_t nextPacketId(Session &ss);
    bool qos2Known(const Session &ss, uint16_t id) const;
    void enqueue(int s, uint32_t msg, uint8_t qos, bool retain);
    void removeEntry(Session &ss, int i);
    void flush(int s);
    void resendInflight(int s);

    // messages
    void deliver(uint16_t from, Bytes topic, Bytes payload, uint8_t qos, bool retain);
    void route(uint16_t from, uint32_t msg);
    void storeRetained(uint32_t msg, Bytes topic);  // msg kNull or empty: delete
    void sendRetained(uint16_t s, Bytes filter, uint8_t grantedQos);
    void deliverLocal(uint32_t msg, uint8_t qos, bool retain);
    int addSubscription(uint16_t s, Bytes filter, uint8_t qos, uint8_t options, bool &replaced);
    bool removeSubscription(uint16_t s, Bytes filter);
    bool allowed(int s, Bytes topic, bool subscribe);

    Conn conns_[TINYMQ_BROKER_MAX_CLIENTS];
    uint8_t rx_[TINYMQ_BROKER_MAX_CLIENTS][TINYMQ_BROKER_RX_BUFFER];
    Session sessions_[TINYMQ_BROKER_MAX_SESSIONS];
    Sub subs_[TINYMQ_BROKER_MAX_SUBSCRIPTIONS];
    uint32_t retained_[TINYMQ_BROKER_MAX_RETAINED];
    uint8_t tx_[TINYMQ_MAX_PACKET];
    char topic_[TINYMQ_MAX_TOPIC_LEN + 1];

#if TINYMQ_BROKER_POOL_BYTES > 0
    alignas(4) uint8_t arena_[TINYMQ_BROKER_POOL_BYTES];
#endif
    uint8_t *heapArena_ = nullptr;
    Pool pool_;
    MessageStore store_;

    uint32_t now_ = 0;
    uint32_t connectTimeout_ = 10000;
    uint32_t sessionExpiry_ = 0;
    uint32_t generatedIds_ = 0;
    uint32_t dropped_ = 0;
    uint8_t maxQos_ = TINYMQ_ENABLE_QOS2_DELIVERY ? 2 : 1;
    bool retainAvailable_ = true;
    bool pendingFlush_ = false;

    AuthHandler onAuth_ = nullptr;
    void *onAuthCtx_ = nullptr;
    AclHandler onAcl_ = nullptr;
    void *onAclCtx_ = nullptr;
    MessageHandler onLocal_ = nullptr;
    void *onLocalCtx_ = nullptr;
    ClientHandler onClient_ = nullptr;
    void *onClientCtx_ = nullptr;
};

}  // namespace tinymq
