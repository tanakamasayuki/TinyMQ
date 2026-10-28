#include "mq_broker.h"

#include <stdio.h>
#include <stdlib.h>

#include "mq_topic.h"

namespace tinymq {

Broker::Broker() : store_(pool_)
{
    for (Conn &c : conns_) {
        c.link = nullptr;
        c.state = kFree;
        c.session = kNone;
        c.rxLen = 0;
        c.broken = false;
    }
    for (Session &s : sessions_)
        s.used = false;
    for (Sub &s : subs_)
        s.session = kNone;
    for (uint32_t &r : retained_)
        r = Pool::kNull;
#if TINYMQ_BROKER_POOL_BYTES > 0
    pool_.begin(arena_, sizeof(arena_));
#endif
}

Broker::~Broker()
{
    free(heapArena_);
}

bool Broker::begin(size_t poolBytes)
{
#if TINYMQ_BROKER_POOL_BYTES > 0
    (void)poolBytes;
    return true;
#else
    if (heapArena_)
        return true;
    heapArena_ = static_cast<uint8_t *>(malloc(poolBytes));
    if (!heapArena_)
        return false;
    pool_.begin(heapArena_, poolBytes);
    return true;
#endif
}

void Broker::setMaxQos(uint8_t qos)
{
    uint8_t cap = TINYMQ_ENABLE_QOS2_DELIVERY ? 2 : 1;
    maxQos_ = qos < cap ? qos : cap;
}

// ---- listener side

int Broker::accept(Link &link, uint32_t now)
{
    now_ = now;
    for (int i = 0; i < TINYMQ_BROKER_MAX_CLIENTS; ++i) {
        Conn &c = conns_[i];
        if (c.state != kFree)
            continue;
        c.link = &link;
        c.state = kWaitConnect;
        c.version = kMqtt311;
        c.session = kNone;
        c.keepAlive = 0;
        c.lastRx = now;
        c.openedAt = now;
        c.rxLen = 0;
        c.broken = false;
        return i;
    }
    return -1;
}

uint8_t *Broker::rxBuffer(int conn, size_t &room)
{
    room = sizeof(rx_[0]) - conns_[conn].rxLen;
    return rx_[conn] + conns_[conn].rxLen;
}

void Broker::rxCommit(int conn, size_t n, uint32_t now)
{
    now_ = now;
    Conn &c = conns_[conn];
    if (c.state == kFree)
        return;
    c.rxLen += n;
    c.lastRx = now;
    size_t pos = 0;
    while (c.state != kFree && !c.broken && pos < c.rxLen) {
        FixedHeader h;
        HeaderResult r = parseFixedHeader(rx_[conn] + pos, c.rxLen - pos, h);
        if (r == HeaderResult::NeedMore)
            break;
        size_t total = h.size + (size_t)h.remaining;
        if (r == HeaderResult::Malformed || total > sizeof(rx_[0])) {
            closeConn(conn, true);
            break;
        }
        if (total > c.rxLen - pos)
            break;
        handlePacket(conn, h, rx_[conn] + pos + h.size);
        pos += total;
    }
    if (c.state != kFree && pos) {
        memmove(rx_[conn], rx_[conn] + pos, c.rxLen - pos);
        c.rxLen -= pos;
    }
    reapBroken();
}

void Broker::linkLost(int conn)
{
    if (isOpen(conn))
        closeConn(conn, true);
    reapBroken();
}

void Broker::poll(uint32_t now)
{
    now_ = now;
    for (int i = 0; i < TINYMQ_BROKER_MAX_CLIENTS; ++i) {
        Conn &c = conns_[i];
        if (c.state == kWaitConnect && now - c.openedAt >= connectTimeout_)
            closeConn(i, false);
        else if (c.state == kActive && c.keepAlive && now - c.lastRx >= (uint32_t)c.keepAlive * 1500)
            closeConn(i, true);
    }
    reapBroken();
    if (sessionExpiry_) {
        for (int s = 0; s < TINYMQ_BROKER_MAX_SESSIONS; ++s) {
            Session &ss = sessions_[s];
            if (ss.used && ss.conn == kNone && now - ss.lastSeen >= sessionExpiry_ * 1000)
                destroySession(s);
        }
    }
    if (pendingFlush_) {
        pendingFlush_ = false;
        for (int s = 0; s < TINYMQ_BROKER_MAX_SESSIONS; ++s)
            if (sessions_[s].used && sessions_[s].conn != kNone)
                flush(s);
        reapBroken();
    }
}

void Broker::flushConn(int conn)
{
    if (isOpen(conn) && conns_[conn].state == kActive) {
        flush(conns_[conn].session);
        reapBroken();
    }
}

void Broker::resendConn(int conn)
{
    if (isOpen(conn) && conns_[conn].state == kActive) {
        resendInflight(conns_[conn].session);
        reapBroken();
    }
}

bool Broker::hasInflight(int conn) const
{
    if (!isOpen(conn) || conns_[conn].state != kActive)
        return false;
    const Session &ss = sessions_[conns_[conn].session];
    for (int i = 0; i < ss.queued; ++i)
        if (ss.queue[i].state != kQueued)
            return true;
    return false;
}

bool Broker::hasQueued(int conn) const
{
    if (!isOpen(conn) || conns_[conn].state != kActive)
        return false;
    const Session &ss = sessions_[conns_[conn].session];
    for (int i = 0; i < ss.queued; ++i)
        if (ss.queue[i].state == kQueued)
            return true;
    return false;
}

bool Broker::setWill(int conn, Bytes topic, Bytes payload, uint8_t qos, bool retain)
{
    if (!isOpen(conn) || conns_[conn].state != kActive || !validTopicName(topic) || topic.len > TINYMQ_MAX_TOPIC_LEN)
        return false;
    uint32_t will = store_.create(topic, Bytes(), payload, qos > 2 ? 2 : qos, retain);
    if (will == Pool::kNull)
        return false;
    Session &ss = sessions_[conns_[conn].session];
    store_.release(ss.will);
    ss.will = will;
    return true;
}

void Broker::clearWill(int conn)
{
    if (!isOpen(conn) || conns_[conn].state != kActive)
        return;
    Session &ss = sessions_[conns_[conn].session];
    store_.release(ss.will);
    ss.will = Pool::kNull;
}

bool Broker::will(int conn, Bytes &topic, Bytes &payload, uint8_t &qos, bool &retain) const
{
    if (!isOpen(conn) || conns_[conn].state != kActive)
        return false;
    uint32_t w = sessions_[conns_[conn].session].will;
    if (w == Pool::kNull)
        return false;
    topic = store_.topic(w);
    payload = store_.payload(w);
    qos = store_.qos(w);
    retain = store_.retain(w);
    return true;
}

// ---- local client

bool Broker::publish(const char *topic, const uint8_t *payload, size_t len, uint8_t qos, bool retain)
{
    return publish(Bytes(topic), Bytes(payload, (uint32_t)len), qos, retain);
}

bool Broker::publish(Bytes t, Bytes payload, uint8_t qos, bool retain)
{
    if (!validTopicName(t) || !validUtf8(t.data, t.len) || t.len > TINYMQ_MAX_TOPIC_LEN || qos > 2)
        return false;
    uint32_t before = dropped_;
    deliver(kLocal, t, payload, qos, retain);
    reapBroken();
    return dropped_ == before;
}

bool Broker::subscribe(const char *filter, uint8_t qos)
{
    Bytes f(filter);
    if (!validTopicFilter(f) || !validUtf8(f.data, f.len) || f.len > TINYMQ_MAX_TOPIC_LEN || qos > 2)
        return false;
    bool replaced;
    if (addSubscription(kLocal, f, qos, qos, replaced) < 0)
        return false;
    sendRetained(kLocal, f, qos);
    return true;
}

bool Broker::unsubscribe(const char *filter)
{
    return removeSubscription(kLocal, Bytes(filter));
}

// ---- statistics

size_t Broker::clients() const
{
    size_t n = 0;
    for (const Conn &c : conns_)
        if (c.state == kActive)
            ++n;
    return n;
}

size_t Broker::sessions() const
{
    size_t n = 0;
    for (const Session &s : sessions_)
        if (s.used)
            ++n;
    return n;
}

size_t Broker::subscriptions() const
{
    size_t n = 0;
    for (const Sub &s : subs_)
        if (s.session != kNone)
            ++n;
    return n;
}

size_t Broker::retained() const
{
    size_t n = 0;
    for (uint32_t r : retained_)
        if (r != Pool::kNull)
            ++n;
    return n;
}

// ---- connections

uint32_t Broker::storeString(Bytes s)
{
    uint32_t h = pool_.alloc(s.len);
    if (h != Pool::kNull && s.len)
        memcpy(pool_.ptr(h), s.data, s.len);
    return h;
}

void Broker::handlePacket(int conn, const FixedHeader &h, const uint8_t *body)
{
    Conn &c = conns_[conn];
    if (c.state == kWaitConnect) {
        if (h.type == PacketType::Connect)
            handleConnect(conn, body, h.remaining);
        else
            closeConn(conn, false);
        return;
    }
    switch (h.type) {
    case PacketType::Publish:
        handlePublish(conn, h, body, h.remaining);
        break;
    case PacketType::Puback:
    case PacketType::Pubrec:
    case PacketType::Pubrel:
    case PacketType::Pubcomp:
        handleAck(conn, h, body, h.remaining);
        break;
    case PacketType::Subscribe:
        handleSubscribe(conn, body, h.remaining);
        break;
    case PacketType::Unsubscribe:
        handleUnsubscribe(conn, body, h.remaining);
        break;
    case PacketType::Pingreq: {
        if (h.remaining != 0) {
            closeConn(conn, true);
            break;
        }
        size_t n = encodePingresp(tx_, sizeof(tx_));
        send(conn, tx_, n);
        break;
    }
    case PacketType::Disconnect: {
        Disconnect d;
        if (decodeDisconnect(h, body, h.remaining, c.version, d) != kSuccess) {
            closeConn(conn, true);
            break;
        }
        closeConn(conn, false);  // a normal disconnect discards the will
        break;
    }
    default:
        closeConn(conn, true);  // a second CONNECT, or a packet only servers send
        break;
    }
}

void Broker::handleConnect(int conn, const uint8_t *body, size_t len)
{
    Conn &c = conns_[conn];
    Connect cp;
    uint8_t r = decodeConnect(body, len, cp);
    // 5.0 sessions arrive with Phase 6; until then the client hears that
    // the version is not supported.
    if (r == kUnsupportedProtocolVersion || (r == kSuccess && cp.version == kMqtt5)) {
        sendConnack(conn, kMqtt311, kUnsupportedProtocolVersion, false);
        closeConn(conn, false);
        return;
    }
    if (r != kSuccess || (cp.hasWill && (!validTopicName(cp.willTopic) || cp.willTopic.len > TINYMQ_MAX_TOPIC_LEN))) {
        closeConn(conn, false);
        return;
    }
    c.version = cp.version;

    char generated[24];
    Bytes id = cp.clientId;
    if (id.len == 0) {
        if (!cp.cleanStart || cp.version == kMqtt31) {
            sendConnack(conn, c.version, kClientIdentifierNotValid, false);
            closeConn(conn, false);
            return;
        }
        snprintf(generated, sizeof(generated), "tinymq-%08lx", (unsigned long)++generatedIds_);
        id = Bytes(generated);
    }

    if (onAuth_) {
        ConnectInfo info;
        info.clientId = id;
        info.hasUsername = cp.hasUsername;
        info.username = cp.username;
        info.hasPassword = cp.hasPassword;
        info.password = cp.password;
        info.version = cp.version;
        uint8_t reason = onAuth_(info, onAuthCtx_);
        if (reason != kSuccess) {
            sendConnack(conn, c.version, reason, false);
            closeConn(conn, false);
            return;
        }
    }

    // the same client id connected elsewhere: that connection ends
    int s = findSession(id);
    if (s >= 0 && sessions_[s].conn != kNone) {
        closeConn(sessions_[s].conn, true);
        s = findSession(id);
    }
    if (s >= 0 && cp.cleanStart) {
        destroySession(s);
        s = -1;
    }
    bool present = s >= 0;
    uint32_t will = Pool::kNull;
    if (cp.hasWill) {
        will = store_.create(cp.willTopic, Bytes(), cp.willPayload, cp.willQos, cp.willRetain);
        if (will == Pool::kNull) {
            sendConnack(conn, c.version, kServerUnavailable, false);
            closeConn(conn, false);
            return;
        }
    }
    if (s < 0) {
        s = newSession(now_);
        uint32_t idHandle = s >= 0 ? storeString(id) : Pool::kNull;
        if (idHandle == Pool::kNull) {
            if (s >= 0)
                sessions_[s].used = false;
            store_.release(will);
            sendConnack(conn, c.version, kServerUnavailable, false);
            closeConn(conn, false);
            return;
        }
        sessions_[s].clientId = idHandle;
        sessions_[s].clientIdLen = (uint16_t)id.len;
    }
    Session &ss = sessions_[s];
    ss.clean = cp.cleanStart;
    ss.conn = (uint16_t)conn;
    pool_.free(ss.username);
    ss.username = cp.hasUsername ? storeString(cp.username) : Pool::kNull;
    ss.usernameLen = ss.username != Pool::kNull ? (uint16_t)cp.username.len : 0;
    store_.release(ss.will);
    ss.will = will;

    c.state = kActive;
    c.session = (uint16_t)s;
    c.keepAlive = cp.keepAlive;
    sendConnack(conn, c.version, kSuccess, present);
    if (onClient_)
        onClient_(string(ss.clientId, ss.clientIdLen), true, onClientCtx_);
    if (present)
        resendInflight(s);
    flush(s);
}

void Broker::handlePublish(int conn, const FixedHeader &h, const uint8_t *body, size_t len)
{
    Conn &c = conns_[conn];
    Publish p;
    if (decodePublish(h, body, len, c.version, p) != kSuccess || !validTopicName(p.topic)) {
        closeConn(conn, true);
        return;
    }
    Session &ss = sessions_[c.session];
    if (p.qos == 2) {
        if (qos2Known(ss, p.packetId)) {
            sendAck(conn, PacketType::Pubrec, p.packetId);  // a resend: delivered already
            return;
        }
        if (ss.qos2InCount >= TINYMQ_BROKER_MAX_QOS2_IN) {
            closeConn(conn, true);
            return;
        }
        ss.qos2In[ss.qos2InCount++] = p.packetId;
    }
    bool sys = p.topic.len >= 4 && memcmp(p.topic.data, "$SYS", 4) == 0;
    if (!sys && p.topic.len <= TINYMQ_MAX_TOPIC_LEN && allowed(c.session, p.topic, false))
        deliver(c.session, p.topic, p.payload, p.qos, p.retain);
    if (p.qos == 1)
        sendAck(conn, PacketType::Puback, p.packetId);
    else if (p.qos == 2)
        sendAck(conn, PacketType::Pubrec, p.packetId);
}

void Broker::handleAck(int conn, const FixedHeader &h, const uint8_t *body, size_t len)
{
    Conn &c = conns_[conn];
    Ack a;
    if (decodeAck(h, body, len, c.version, a) != kSuccess) {
        closeConn(conn, true);
        return;
    }
    int s = c.session;
    Session &ss = sessions_[s];
    if (h.type == PacketType::Pubrel) {
        for (uint8_t i = 0; i < ss.qos2InCount; ++i)
            if (ss.qos2In[i] == a.packetId) {
                ss.qos2In[i] = ss.qos2In[--ss.qos2InCount];
                break;
            }
        sendAck(conn, PacketType::Pubcomp, a.packetId);
        return;
    }
    uint8_t want = h.type == PacketType::Puback ? kWaitPuback : h.type == PacketType::Pubrec ? kWaitPubrec : kWaitPubcomp;
    for (int i = 0; i < ss.queued; ++i) {
        Entry &e = ss.queue[i];
        if (e.state != want || e.packetId != a.packetId)
            continue;
        if (h.type == PacketType::Pubrec) {
            store_.release(e.msg);
            e.msg = Pool::kNull;
            e.state = kWaitPubcomp;
        } else {
            removeEntry(ss, i);
            --ss.inflight;
            flush(s);
        }
        break;
    }
    if (h.type == PacketType::Pubrec)
        sendAck(conn, PacketType::Pubrel, a.packetId);
}

void Broker::handleSubscribe(int conn, const uint8_t *body, size_t len)
{
    Conn &c = conns_[conn];
    Subscribe sp;
    if (decodeSubscribe(body, len, c.version, sp) != kSuccess) {
        closeConn(conn, true);
        return;
    }
    // the reason codes are collected at the end of tx_; encoding moves them
    // forward into the SUBACK (Writer uses memmove)
    size_t count = 0;
    size_t pos = 0;
    Bytes filter;
    SubOptions opts;
    while (nextSubscription(sp, pos, filter, opts))
        ++count;
    if (count + 16 > sizeof(tx_)) {
        closeConn(conn, true);
        return;
    }
    uint8_t *codes = tx_ + sizeof(tx_) - count;
    pos = 0;
    for (size_t i = 0; nextSubscription(sp, pos, filter, opts); ++i) {
        codes[i] = 0x80;
        if (!validTopicFilter(filter) || filter.len > TINYMQ_MAX_TOPIC_LEN || !allowed(c.session, filter, true))
            continue;
        uint8_t granted = opts.qos < maxQos_ ? opts.qos : maxQos_;
        bool replaced;
        if (addSubscription(c.session, filter, granted, opts.encode(), replaced) >= 0)
            codes[i] = granted;
    }
    SubAck a;
    a.packetId = sp.packetId;
    a.reasons = Bytes(codes, (uint32_t)count);
    size_t n = encodeSubAck(tx_, sizeof(tx_), c.version, PacketType::Suback, a);
    // the codes were overwritten by the SUBACK; read them back from it
    uint8_t granted[64];
    size_t keep = count < sizeof(granted) ? count : sizeof(granted);
    memcpy(granted, tx_ + n - count, keep);
    if (!send(conn, tx_, n))
        return;
    // retained messages for each new subscription, after the SUBACK
    pos = 0;
    for (size_t i = 0; i < keep && nextSubscription(sp, pos, filter, opts); ++i)
        if (granted[i] < 0x80)
            sendRetained(c.session, filter, granted[i]);
}

void Broker::handleUnsubscribe(int conn, const uint8_t *body, size_t len)
{
    Conn &c = conns_[conn];
    Unsubscribe u;
    if (decodeUnsubscribe(body, len, c.version, u) != kSuccess) {
        closeConn(conn, true);
        return;
    }
    size_t pos = 0;
    Bytes filter;
    while (nextUnsubscription(u, pos, filter))
        removeSubscription(c.session, filter);
    SubAck a;
    a.packetId = u.packetId;
    size_t n = encodeSubAck(tx_, sizeof(tx_), c.version, PacketType::Unsuback, a);
    send(conn, tx_, n);
}

void Broker::closeConn(int conn, bool publishWill)
{
    Conn &c = conns_[conn];
    if (c.state == kFree)
        return;
    uint16_t s = c.session;
    Link *link = c.link;
    c.state = kFree;
    c.session = kNone;
    c.rxLen = 0;
    c.link = nullptr;
    c.broken = false;
    link->close();
    if (s == kNone)
        return;
    Session &ss = sessions_[s];
    if (ss.will != Pool::kNull) {
        uint32_t will = ss.will;
        ss.will = Pool::kNull;
        if (publishWill)
            route(s, will);
        store_.release(will);
    }
    if (onClient_)
        onClient_(string(ss.clientId, ss.clientIdLen), false, onClientCtx_);
    detachSession(s, now_);
}

void Broker::reapBroken()
{
    for (int i = 0; i < TINYMQ_BROKER_MAX_CLIENTS; ++i)
        if (conns_[i].state != kFree && conns_[i].broken)
            closeConn(i, true);
}

bool Broker::send(int conn, const uint8_t *data, size_t len)
{
    Conn &c = conns_[conn];
    if (c.state == kFree || c.broken)
        return false;
    if (c.link->write(data, len) != len) {
        c.broken = true;  // closed by reapBroken(), outside whatever is iterating now
        return false;
    }
    return true;
}

bool Broker::sendAck(int conn, PacketType type, uint16_t packetId)
{
    Ack a;
    a.packetId = packetId;
    size_t n = encodeAck(tx_, sizeof(tx_), conns_[conn].version, type, a);
    return send(conn, tx_, n);
}

bool Broker::sendConnack(int conn, uint8_t version, uint8_t reason, bool sessionPresent)
{
    Connack a;
    a.reason = reason;
    a.sessionPresent = sessionPresent;
    size_t n = encodeConnack(tx_, sizeof(tx_), version, a);
    return send(conn, tx_, n);
}

bool Broker::sendPublish(int conn, uint32_t msg, uint8_t qos, uint16_t packetId, bool retain, bool dup)
{
    Publish p;
    p.topic = store_.topic(msg);
    p.payload = store_.payload(msg);
    p.qos = qos;
    p.retain = retain;
    p.dup = dup;
    p.packetId = packetId;
    size_t head = encodePublishHead(tx_, sizeof(tx_), conns_[conn].version, p);
    if (head == 0)
        return false;
    size_t len = p.payload.len;
    if (head + len <= sizeof(tx_)) {
        if (len)
            memcpy(tx_ + head, p.payload.data, len);
        return send(conn, tx_, head + len);
    }
    return send(conn, tx_, head) && send(conn, p.payload.data, len);
}

// ---- sessions

int Broker::findSession(Bytes clientId) const
{
    for (int s = 0; s < TINYMQ_BROKER_MAX_SESSIONS; ++s) {
        const Session &ss = sessions_[s];
        if (ss.used && string(ss.clientId, ss.clientIdLen).equals(clientId))
            return s;
    }
    return -1;
}

int Broker::newSession(uint32_t now)
{
    int pick = -1;
    for (int s = 0; s < TINYMQ_BROKER_MAX_SESSIONS; ++s)
        if (!sessions_[s].used) {
            pick = s;
            break;
        }
    if (pick < 0) {
        // evict the offline session seen longest ago
        uint32_t oldest = 0;
        for (int s = 0; s < TINYMQ_BROKER_MAX_SESSIONS; ++s) {
            Session &ss = sessions_[s];
            if (ss.conn == kNone && (pick < 0 || now - ss.lastSeen > oldest)) {
                pick = s;
                oldest = now - ss.lastSeen;
            }
        }
        if (pick < 0)
            return -1;
        destroySession(pick);
    }
    Session &ss = sessions_[pick];
    ss.used = true;
    ss.clean = true;
    ss.conn = kNone;
    ss.clientId = Pool::kNull;
    ss.clientIdLen = 0;
    ss.username = Pool::kNull;
    ss.usernameLen = 0;
    ss.will = Pool::kNull;
    ss.lastSeen = now;
    ss.lastPacketId = 0;
    ss.queued = 0;
    ss.inflight = 0;
    ss.qos2InCount = 0;
    return pick;
}

void Broker::destroySession(int s)
{
    Session &ss = sessions_[s];
    for (Sub &sub : subs_)
        if (sub.session == s) {
            pool_.free(sub.filter);
            sub.session = kNone;
        }
    for (int i = 0; i < ss.queued; ++i)
        store_.release(ss.queue[i].msg);
    ss.queued = 0;
    ss.inflight = 0;
    ss.qos2InCount = 0;
    store_.release(ss.will);
    ss.will = Pool::kNull;
    pool_.free(ss.clientId);
    pool_.free(ss.username);
    ss.used = false;
}

void Broker::detachSession(int s, uint32_t now)
{
    Session &ss = sessions_[s];
    ss.conn = kNone;
    ss.lastSeen = now;
    if (ss.clean) {
        destroySession(s);
        return;
    }
    // QoS 0 messages are not kept for offline clients
    for (int i = 0; i < ss.queued;) {
        if (ss.queue[i].state == kQueued && ss.queue[i].qos == 0)
            removeEntry(ss, i);
        else
            ++i;
    }
}

uint16_t Broker::nextPacketId(Session &ss)
{
    for (;;) {
        if (++ss.lastPacketId == 0)
            ss.lastPacketId = 1;
        bool used = false;
        for (int i = 0; i < ss.queued; ++i)
            if (ss.queue[i].state != kQueued && ss.queue[i].packetId == ss.lastPacketId) {
                used = true;
                break;
            }
        if (!used)
            return ss.lastPacketId;
    }
}

bool Broker::qos2Known(const Session &ss, uint16_t id) const
{
    for (uint8_t i = 0; i < ss.qos2InCount; ++i)
        if (ss.qos2In[i] == id)
            return true;
    return false;
}

void Broker::enqueue(int s, uint32_t msg, uint8_t qos, bool retain)
{
    Session &ss = sessions_[s];
    if (ss.conn == kNone && qos == 0)
        return;
    if (ss.queued >= TINYMQ_BROKER_MAX_QUEUED) {
        ++dropped_;
        return;
    }
    Entry &e = ss.queue[ss.queued++];
    store_.addRef(msg);
    e.msg = msg;
    e.packetId = 0;
    e.qos = qos;
    e.state = kQueued;
    e.retain = retain;
}

void Broker::removeEntry(Session &ss, int i)
{
    store_.release(ss.queue[i].msg);
    for (int k = i + 1; k < ss.queued; ++k)
        ss.queue[k - 1] = ss.queue[k];
    --ss.queued;
}

void Broker::flush(int s)
{
    Session &ss = sessions_[s];
    if (ss.conn == kNone)
        return;
    int conn = ss.conn;
    Conn &c = conns_[conn];
    if (c.state != kActive)
        return;
    for (int i = 0; i < ss.queued;) {
        Entry &e = ss.queue[i];
        if (e.state != kQueued) {
            ++i;
            continue;
        }
        if (e.qos > 0 && ss.inflight >= TINYMQ_BROKER_MAX_INFLIGHT)
            break;  // later messages wait too, to keep the order
        size_t need = 9 + store_.topic(e.msg).len + store_.payload(e.msg).len;
        if (c.link->writable() < need) {
            pendingFlush_ = true;
            break;
        }
        if (e.qos == 0) {
            bool ok = sendPublish(conn, e.msg, 0, 0, e.retain, false);
            removeEntry(ss, i);
            if (!ok)
                break;
            continue;
        }
        e.packetId = nextPacketId(ss);
        e.state = e.qos == 1 ? kWaitPuback : kWaitPubrec;
        ++ss.inflight;
        if (!sendPublish(conn, e.msg, e.qos, e.packetId, e.retain, false))
            break;
        ++i;
    }
}

void Broker::resendInflight(int s)
{
    Session &ss = sessions_[s];
    int conn = ss.conn;
    for (int i = 0; i < ss.queued; ++i) {
        Entry &e = ss.queue[i];
        if (e.state == kWaitPubcomp)
            sendAck(conn, PacketType::Pubrel, e.packetId);
        else if (e.state != kQueued)
            sendPublish(conn, e.msg, e.qos, e.packetId, e.retain, true);
    }
}

// ---- messages

void Broker::deliver(uint16_t from, Bytes topic, Bytes payload, uint8_t qos, bool retain)
{
    if (!retain || !retainAvailable_) {
        // skip the copy when nobody would receive it
        bool any = false;
        for (const Sub &sub : subs_)
            if (sub.session != kNone && topicMatches(string(sub.filter, sub.filterLen), topic)) {
                any = true;
                break;
            }
        if (!any)
            return;
    }
    uint32_t msg = store_.create(topic, Bytes(), payload, qos, retain);
    if (msg == Pool::kNull) {
        ++dropped_;
        if (retain && retainAvailable_ && payload.len == 0)
            storeRetained(Pool::kNull, topic);  // deleting needs no room
        return;
    }
    route(from, msg);
    store_.release(msg);
}

void Broker::route(uint16_t from, uint32_t msg)
{
    (void)from;  // for No Local (5.0)
    Bytes topic = store_.topic(msg);
    uint8_t qos = store_.qos(msg);
    if (store_.retain(msg) && retainAvailable_)
        storeRetained(msg, topic);

    // one delivery per session, at the highest QoS of its matching subscriptions
    uint8_t best[TINYMQ_BROKER_MAX_SESSIONS];
    memset(best, 0xFF, sizeof(best));
    int local = -1;
    for (const Sub &sub : subs_) {
        if (sub.session == kNone || !topicMatches(string(sub.filter, sub.filterLen), topic))
            continue;
        if (sub.session == kLocal) {
            if (sub.qos > local)
                local = sub.qos;
        } else if (best[sub.session] == 0xFF || sub.qos > best[sub.session]) {
            best[sub.session] = sub.qos;
        }
    }
    for (int s = 0; s < TINYMQ_BROKER_MAX_SESSIONS; ++s) {
        if (best[s] == 0xFF)
            continue;
        enqueue(s, msg, qos < best[s] ? qos : best[s], false);
        flush(s);
    }
    if (local >= 0)
        deliverLocal(msg, qos < local ? qos : (uint8_t)local, false);
}

void Broker::storeRetained(uint32_t msg, Bytes topic)
{
    int slot = -1;
    for (int i = 0; i < TINYMQ_BROKER_MAX_RETAINED; ++i) {
        if (retained_[i] != Pool::kNull && store_.topic(retained_[i]).equals(topic)) {
            store_.release(retained_[i]);
            retained_[i] = Pool::kNull;
            slot = i;
            break;
        }
    }
    if (msg == Pool::kNull || store_.payload(msg).len == 0)
        return;  // an empty retained message deletes
    if (slot < 0)
        for (int i = 0; i < TINYMQ_BROKER_MAX_RETAINED; ++i)
            if (retained_[i] == Pool::kNull) {
                slot = i;
                break;
            }
    if (slot < 0) {
        ++dropped_;
        return;
    }
    store_.addRef(msg);
    retained_[slot] = msg;
}

void Broker::sendRetained(uint16_t s, Bytes filter, uint8_t grantedQos)
{
    for (uint32_t r : retained_) {
        if (r == Pool::kNull || !topicMatches(filter, store_.topic(r)))
            continue;
        uint8_t qos = store_.qos(r) < grantedQos ? store_.qos(r) : grantedQos;
        if (s == kLocal)
            deliverLocal(r, qos, true);
        else
            enqueue(s, r, qos, true);
    }
    if (s != kLocal)
        flush(s);
}

void Broker::deliverLocal(uint32_t msg, uint8_t qos, bool retain)
{
    if (!onLocal_)
        return;
    Bytes topic = store_.topic(msg);
    Bytes payload = store_.payload(msg);
    memcpy(topic_, topic.data, topic.len);  // topics are at most TINYMQ_MAX_TOPIC_LEN
    topic_[topic.len] = 0;
    Message m;
    m.topic = topic_;
    m.topicLen = (uint16_t)topic.len;
    m.payload = payload.data;
    m.payloadLen = payload.len;
    m.qos = qos;
    m.retain = retain;
    m.dup = false;
    m.props = Bytes();
    onLocal_(m, onLocalCtx_);
}

int Broker::addSubscription(uint16_t s, Bytes filter, uint8_t qos, uint8_t options, bool &replaced)
{
    replaced = false;
    int free = -1;
    for (int i = 0; i < TINYMQ_BROKER_MAX_SUBSCRIPTIONS; ++i) {
        Sub &sub = subs_[i];
        if (sub.session == kNone) {
            if (free < 0)
                free = i;
            continue;
        }
        if (sub.session == s && string(sub.filter, sub.filterLen).equals(filter)) {
            sub.qos = qos;
            sub.options = options;
            replaced = true;
            return i;
        }
    }
    if (free < 0)
        return -1;
    uint32_t h = storeString(filter);
    if (h == Pool::kNull)
        return -1;
    Sub &sub = subs_[free];
    sub.session = s;
    sub.qos = qos;
    sub.options = options;
    sub.filter = h;
    sub.filterLen = (uint16_t)filter.len;
    return free;
}

bool Broker::removeSubscription(uint16_t s, Bytes filter)
{
    for (Sub &sub : subs_) {
        if (sub.session == s && string(sub.filter, sub.filterLen).equals(filter)) {
            pool_.free(sub.filter);
            sub.session = kNone;
            return true;
        }
    }
    return false;
}

bool Broker::allowed(int s, Bytes topic, bool subscribe)
{
    if (!onAcl_)
        return true;
    const Session &ss = sessions_[s];
    return onAcl_(string(ss.clientId, ss.clientIdLen), string(ss.username, ss.usernameLen), topic, subscribe, onAclCtx_);
}

}  // namespace tinymq
