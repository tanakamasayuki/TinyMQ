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
#include "mq_props.h"

namespace tinymq {

// What a client presented in CONNECT, for the authentication callback.
struct ConnectInfo {
    Bytes clientId;
    bool hasUsername;
    Bytes username;
    bool hasPassword;
    Bytes password;
    uint8_t version;
    Bytes props;  // 5.0 CONNECT properties (user properties, ...)
};

// Where saveState() writes and loadState() reads (a file, flash, ...)
class StateSink {
public:
    virtual ~StateSink() {}
    virtual bool write(const uint8_t *data, size_t len) = 0;
};

class StateSource {
public:
    virtual ~StateSource() {}
    // Reads exactly `len` bytes; false at the end or on an error
    virtual bool read(uint8_t *data, size_t len) = 0;
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
    // A retained message was stored or deleted (a cue to save the state)
    typedef void (*RetainedHandler)(Bytes topic, bool deleted, void *context);

    static const uint16_t kNone = 0xFFFF;

    Broker();
    ~Broker();

    // Prepares the message pool: the static one, or `poolBytes` allocated
    // here once when TINYMQ_BROKER_POOL_BYTES is 0. False if that fails.
    bool begin(size_t poolBytes = 0);
    // Closes every connection (no wills) and forgets everything: sessions,
    // subscriptions (the local client's and the taps' too), retained messages.
    void end();

    // ---- settings
    void setMaxQos(uint8_t qos);                         // highest QoS granted to subscribers (default 2)
    void setConnectTimeout(uint32_t ms) { connectTimeout_ = ms; }  // from accept() to CONNECT (default 10 s)
    void setRetainAvailable(bool on) { retainAvailable_ = on; }    // false: retained messages are not kept
    // Longest a session outlives its connection (5.0 clients ask for theirs;
    // 3.1.1 persistent sessions take this). 0 (default): no limit, kept
    // until evicted for a new session.
    void setSessionExpiry(uint32_t seconds) { sessionExpiry_ = seconds; }
    // Publishes the broker's state under $SYS/broker/ (retained) every
    // `seconds`, like mosquitto. 0 (default): off, and the $SYS messages
    // already retained are deleted.
    void setSysInterval(uint32_t seconds);
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

    // ---- taps: internal subscribers for bridges. A tap never receives what
    // it published itself, and gets the retain flag as published.
    int addTap(MessageHandler h, void *context);  // tap number, or -1
    // Subscribing again with the same filter delivers the retained messages again.
    bool tapSubscribe(int tap, const char *filter, uint8_t qos);
    bool tapPublish(int tap, Bytes topic, Bytes payload, uint8_t qos, bool retain, Bytes props = Bytes());

    // ---- persistence
    // Writes the retained messages and the persistent sessions (their
    // subscriptions and the QoS 1 / 2 messages not yet delivered; connected
    // sessions as if they ended now). False if the sink failed.
    bool saveState(StateSink &sink);
    // Reads what saveState() wrote, before clients connect. Messages in
    // flight then are delivered again from the start (QoS 1 may arrive twice).
    // False if the data is not a saved state or something did not fit.
    bool loadState(StateSource &source);
    void onRetainedChange(RetainedHandler h, void *context = nullptr) { onRetained_ = h; onRetainedCtx_ = context; }

    // ---- statistics
    size_t clients() const;        // connected
    size_t sessions() const;       // connected and offline persistent
    size_t subscriptions() const;
    size_t retained() const;
    size_t poolUsed() const { return pool_.used(); }
    size_t poolCapacity() const { return pool_.capacity(); }
    uint32_t droppedMessages() const { return dropped_; }  // queue full, pool full or no room for a retained message
    uint32_t receivedMessages() const { return received_; }  // PUBLISH from clients
    uint32_t sentMessages() const { return sent_; }          // PUBLISH to clients

private:
    enum ConnState : uint8_t { kFree, kWaitConnect, kActive };
    enum EntryState : uint8_t { kQueued, kWaitPuback, kWaitPubrec, kWaitPubcomp };
    static const uint16_t kLocal = 0xFFFE;
    static const uint16_t kTapBase = 0xFF00;  // kTapBase + tap number
    static bool isInternal(uint16_t s) { return s >= kTapBase && s != kNone; }

    static const uint32_t kNever = 0xFFFFFFFF;

    struct Conn {
        Link *link;
        uint8_t state;
        uint8_t version;
        uint16_t session;
        uint16_t keepAlive;
        uint16_t receiveMax;  // 5.0: QoS 1 / 2 deliveries the client takes at once
        uint32_t maxPacket;   // 5.0: largest packet the client takes
        uint32_t lastRx;
        uint32_t openedAt;
        size_t rxLen;
        bool broken;  // a write failed; closed by reapBroken()
#if TINYMQ_BROKER_TOPIC_ALIAS_MAX > 0
        uint32_t alias[TINYMQ_BROKER_TOPIC_ALIAS_MAX];  // topics by alias - 1 (pool handles)
        uint16_t aliasLen[TINYMQ_BROKER_TOPIC_ALIAS_MAX];
#endif
    };

    struct Entry {
        uint32_t msg;       // kNull once PUBREC arrived
        uint32_t subId;     // 5.0 subscription identifier, 0 if none
        uint16_t packetId;  // 0 while queued
        uint8_t qos;
        uint8_t state;
        bool retain;        // the retain flag to send
    };

    struct Session {
        bool used;
        uint8_t version;
        uint16_t conn;
        uint32_t clientId;  // pool handles of the strings
        uint16_t clientIdLen;
        uint32_t username;
        uint16_t usernameLen;
        uint32_t expiry;    // seconds the session outlives its connection; kNever
        uint32_t will;      // message, kNull if none
        uint32_t willDelay; // seconds
        uint32_t willAt;    // when a delayed will is due (seconds), if willPending
        bool willPending;
        uint32_t lastSeen;  // seconds
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
        uint8_t options;   // SubOptions::encode()
        uint32_t subId;    // 5.0 subscription identifier, 0 if none
        uint32_t filter;   // pool handle (without $share/<group>/)
        uint16_t filterLen;
        uint32_t group;    // shared subscription group, kNull if not shared
        uint16_t groupLen;
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
    void handleDisconnect(int conn, const FixedHeader &h, const uint8_t *body, size_t len);
    // Ends a connection; a 5.0 client is told `reason` first (when nonzero)
    void closeConn(int conn, bool publishWill, uint8_t reason = 0);
    void reapBroken();
    bool send(int conn, const uint8_t *data, size_t len);
    bool sendAck(int conn, PacketType type, uint16_t packetId, uint8_t reason = kSuccess);
    bool sendConnack(int conn, uint8_t version, uint8_t reason, bool sessionPresent, Bytes props = Bytes());
    // 0: sent, 1: dropped (expired or too large for the client), -1: write failed
    int sendPublish(int conn, const Entry &e, bool dup);
    uint8_t inflightLimit(int conn) const;

    // sessions
    int findSession(Bytes clientId) const;
    int newSession(uint32_t now);
    void destroySession(int s);
    void clearSessionState(int s);
    void detachSession(int s);
    void publishWill(int s);
    void tick(uint32_t now);
    void publishSys();
    uint16_t nextPacketId(Session &ss);
    bool qos2Known(const Session &ss, uint16_t id) const;
    void enqueue(int s, uint32_t msg, uint8_t qos, bool retain, uint32_t subId = 0);
    void removeEntry(Session &ss, int i);
    void flush(int s);
    void resendInflight(int s);

    // messages
    // Whether any subscriber took it
    bool deliver(uint16_t from, Bytes topic, Bytes payload, uint8_t qos, bool retain, Bytes props = Bytes(), uint32_t expiry = 0);
    bool route(uint16_t from, uint32_t msg);
    bool expired(uint32_t msg) const;
    void storeRetained(uint32_t msg, Bytes topic);  // msg kNull or empty: delete
    void sendRetained(uint16_t s, Bytes filter, uint8_t grantedQos, uint32_t subId);
    void deliverLocal(uint32_t msg, uint8_t qos, bool retain);
    void deliverTo(MessageHandler h, void *ctx, uint32_t msg, uint8_t qos, bool retain);
    // `filter` is the full filter, $share/<group>/ included
    int addSubscription(uint16_t s, Bytes filter, uint8_t qos, uint8_t options, uint32_t subId, bool &replaced);
    bool removeSubscription(uint16_t s, Bytes filter);
    int findSubscription(uint16_t s, Bytes filter) const;
    bool allowed(int s, Bytes topic, bool subscribe);
    bool saveMessage(StateSink &sink, uint32_t msg, uint8_t qos, bool retain, uint32_t subId);

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
    uint32_t seconds_ = 1;  // uptime in seconds from 1, carried past the millis() wrap
    uint32_t lastTickMs_ = 0;
    uint32_t tickRest_ = 0;
    uint32_t sharedNext_ = 0;
    uint32_t connectTimeout_ = 10000;
    uint32_t sessionExpiry_ = 0;
    uint32_t generatedIds_ = 0;
    uint32_t dropped_ = 0;
    uint32_t received_ = 0;
    uint32_t sent_ = 0;
    uint32_t sysInterval_ = 0;
    uint32_t sysLast_ = 0;
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
    RetainedHandler onRetained_ = nullptr;
    void *onRetainedCtx_ = nullptr;
    MessageHandler tapHandler_[TINYMQ_BROKER_MAX_TAPS] = {};
    void *tapCtx_[TINYMQ_BROKER_MAX_TAPS] = {};
};

}  // namespace tinymq
