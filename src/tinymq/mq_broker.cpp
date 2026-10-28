#include "mq_broker.h"

#include <stdio.h>
#include <stdlib.h>

#include "mq_topic.h"

namespace tinymq {

// Properties of an incoming PUBLISH that are not kept with the message:
// the alias is the connection's, subscription identifiers are added per
// delivery, and the expiry is kept as a time and sent as what is left.
static const uint8_t kNotKept[] = {kTopicAlias, kSubscriptionIdentifier, kMessageExpiryInterval};
// A will's properties, likewise (the delay is the broker's business)
static const uint8_t kWillNotKept[] = {kWillDelayInterval, kMessageExpiryInterval};

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

void Broker::tick(uint32_t now)
{
    now_ = now;
    tickRest_ += now - lastTickMs_;
    lastTickMs_ = now;
    seconds_ += tickRest_ / 1000;
    tickRest_ %= 1000;
}

// ---- listener side

int Broker::accept(Link &link, uint32_t now)
{
    tick(now);
    for (int i = 0; i < TINYMQ_BROKER_MAX_CLIENTS; ++i) {
        Conn &c = conns_[i];
        if (c.state != kFree)
            continue;
        c.link = &link;
        c.state = kWaitConnect;
        c.version = kMqtt311;
        c.session = kNone;
        c.keepAlive = 0;
        c.receiveMax = 0xFFFF;
        c.maxPacket = 0xFFFFFFFF;
        c.lastRx = now;
        c.openedAt = now;
        c.rxLen = 0;
        c.broken = false;
#if TINYMQ_BROKER_TOPIC_ALIAS_MAX > 0
        for (uint32_t &a : c.alias)
            a = Pool::kNull;
#endif
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
    tick(now);
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
        if (r == HeaderResult::Malformed) {
            closeConn(conn, true, kMalformedPacket);
            break;
        }
        if (total > sizeof(rx_[0])) {
            closeConn(conn, true, kPacketTooLarge);
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
    tick(now);
    for (int i = 0; i < TINYMQ_BROKER_MAX_CLIENTS; ++i) {
        Conn &c = conns_[i];
        if (c.state == kWaitConnect && now - c.openedAt >= connectTimeout_)
            closeConn(i, false);
        else if (c.state == kActive && c.keepAlive && now - c.lastRx >= (uint32_t)c.keepAlive * 1500)
            closeConn(i, true, kKeepAliveTimeout);
    }
    reapBroken();
    for (int s = 0; s < TINYMQ_BROKER_MAX_SESSIONS; ++s) {
        Session &ss = sessions_[s];
        if (!ss.used || ss.conn != kNone)
            continue;
        if (ss.willPending && (int32_t)(seconds_ - ss.willAt) >= 0)
            publishWill(s);
        if (ss.expiry != kNever && seconds_ - ss.lastSeen >= ss.expiry) {
            publishWill(s);  // a will still delayed goes out as the session ends
            destroySession(s);
        }
    }
    reapBroken();
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
    Bytes group, inner;
    if (!validTopicFilter(f) || !validUtf8(f.data, f.len) || f.len > TINYMQ_MAX_TOPIC_LEN || qos > 2 || parseSharedFilter(f, group, inner))
        return false;
    bool replaced;
    SubOptions o;
    o.qos = qos;
    if (addSubscription(kLocal, f, qos, o.encode(), 0, replaced) < 0)
        return false;
    sendRetained(kLocal, f, qos, 0);
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
            closeConn(conn, true, kMalformedPacket);
            break;
        }
        size_t n = encodePingresp(tx_, sizeof(tx_));
        send(conn, tx_, n);
        break;
    }
    case PacketType::Disconnect:
        handleDisconnect(conn, h, body, h.remaining);
        break;
    default:
        // a second CONNECT, AUTH (no enhanced authentication), or a packet only servers send
        closeConn(conn, true, kProtocolError);
        break;
    }
}

void Broker::handleConnect(int conn, const uint8_t *body, size_t len)
{
    Conn &c = conns_[conn];
    Connect cp;
    uint8_t r = decodeConnect(body, len, cp);
    if (r == kUnsupportedProtocolVersion || (r == kSuccess && cp.version == kMqtt5 && !TINYMQ_ENABLE_MQTT5)) {
        sendConnack(conn, kMqtt311, kUnsupportedProtocolVersion, false);
        closeConn(conn, false);
        return;
    }
    bool v5 = r == kSuccess && cp.version == kMqtt5;
    if (r == kSuccess && v5) {
        r = checkProps(cp.props, kInConnect);
        if (r == kSuccess && cp.hasWill)
            r = checkProps(cp.willProps, kInWill);
    }
    if (r == kSuccess && cp.hasWill && (!validTopicName(cp.willTopic) || cp.willTopic.len > TINYMQ_MAX_TOPIC_LEN))
        r = kTopicNameInvalid;
    if (r != kSuccess) {
        if (v5)
            sendConnack(conn, kMqtt5, r, false);  // 3.1.1 has no code for these: just close
        closeConn(conn, false);
        return;
    }
    c.version = cp.version;

    Property p;
    uint32_t expiry;
    if (v5) {
        if (findProp(cp.props, kAuthenticationMethod, p)) {
            sendConnack(conn, c.version, kBadAuthenticationMethod, false);
            closeConn(conn, false);
            return;
        }
        expiry = findProp(cp.props, kSessionExpiryInterval, p) ? p.value : 0;
        c.receiveMax = findProp(cp.props, kReceiveMaximum, p) ? (uint16_t)p.value : 0xFFFF;
        c.maxPacket = findProp(cp.props, kMaximumPacketSize, p) ? p.value : 0xFFFFFFFF;
    } else {
        expiry = cp.cleanStart ? 0 : kNever;
    }
    bool capped = sessionExpiry_ && expiry > sessionExpiry_;
    if (capped)
        expiry = sessionExpiry_;

    char generated[24];
    Bytes id = cp.clientId;
    bool assigned = false;
    if (id.len == 0) {
        if ((!v5 && !cp.cleanStart) || cp.version == kMqtt31) {
            sendConnack(conn, c.version, kClientIdentifierNotValid, false);
            closeConn(conn, false);
            return;
        }
        snprintf(generated, sizeof(generated), "tinymq-%08lx", (unsigned long)++generatedIds_);
        id = Bytes(generated);
        assigned = true;
    }

    if (onAuth_) {
        ConnectInfo info;
        info.clientId = id;
        info.hasUsername = cp.hasUsername;
        info.username = cp.username;
        info.hasPassword = cp.hasPassword;
        info.password = cp.password;
        info.version = cp.version;
        info.props = cp.props;
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
        closeConn(sessions_[s].conn, true, kSessionTakenOver);
        s = findSession(id);
    }
    if (s >= 0 && cp.cleanStart) {
        publishWill(s);  // a delayed will of the old session goes out now
        destroySession(s);
        s = -1;
    }
    bool present = s >= 0;
    uint32_t will = Pool::kNull;
    uint32_t willDelay = 0;
    if (cp.hasWill) {
        uint8_t buf[256];
        PropWriter w(buf, sizeof(buf));
        w.copy(cp.willProps, kWillNotKept, sizeof(kWillNotKept));
        uint32_t willExpiry = 0;
        if (findProp(cp.willProps, kMessageExpiryInterval, p))
            willExpiry = p.value;  // counted from when the will is published: kept as an interval here
        if (findProp(cp.willProps, kWillDelayInterval, p))
            willDelay = p.value;
        will = w.ok() ? store_.create(cp.willTopic, w.result(), cp.willPayload, cp.willQos, cp.willRetain, willExpiry) : Pool::kNull;
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
    if (ss.willPending) {
        // back before the delayed will was due: it is not published
        store_.release(ss.will);
        ss.will = Pool::kNull;
        ss.willPending = false;
    }
    ss.version = c.version;
    ss.expiry = expiry;
    ss.conn = (uint16_t)conn;
    pool_.free(ss.username);
    ss.username = cp.hasUsername ? storeString(cp.username) : Pool::kNull;
    ss.usernameLen = ss.username != Pool::kNull ? (uint16_t)cp.username.len : 0;
    store_.release(ss.will);
    ss.will = will;
    ss.willDelay = willDelay;

    c.state = kActive;
    c.session = (uint16_t)s;
    c.keepAlive = cp.keepAlive;
    uint8_t propBuf[64];
    PropWriter cw(propBuf, sizeof(propBuf));
    if (v5) {
        if (capped)
            cw.u32(kSessionExpiryInterval, expiry);
        if (assigned)
            cw.string(kAssignedClientIdentifier, id);
        cw.u16(kReceiveMaximum, TINYMQ_BROKER_MAX_QOS2_IN);
        cw.u32(kMaximumPacketSize, sizeof(rx_[0]));
#if TINYMQ_BROKER_TOPIC_ALIAS_MAX > 0
        cw.u16(kTopicAliasMaximum, TINYMQ_BROKER_TOPIC_ALIAS_MAX);
#endif
        if (!retainAvailable_)
            cw.byte(kRetainAvailable, 0);
        if (!TINYMQ_ENABLE_SHARED_SUB)
            cw.byte(kSharedSubscriptionAvailable, 0);
    }
    sendConnack(conn, c.version, kSuccess, present, cw.result());
    if (onClient_)
        onClient_(string(ss.clientId, ss.clientIdLen), true, onClientCtx_);
    if (present)
        resendInflight(s);
    flush(s);
}

void Broker::handlePublish(int conn, const FixedHeader &h, const uint8_t *body, size_t len)
{
    Conn &c = conns_[conn];
    bool v5 = c.version == kMqtt5;
    Publish p;
    uint8_t r = decodePublish(h, body, len, c.version, p);
    Property prop;
    if (r == kSuccess && v5) {
        r = checkProps(p.props, kInPublish);
        if (r == kSuccess && findProp(p.props, kSubscriptionIdentifier, prop))
            r = kProtocolError;  // only servers send it
    }
    if (r != kSuccess) {
        closeConn(conn, true, r);
        return;
    }
    Bytes topic = p.topic;
    if (v5 && findProp(p.props, kTopicAlias, prop)) {
#if TINYMQ_BROKER_TOPIC_ALIAS_MAX > 0
        if (prop.value > TINYMQ_BROKER_TOPIC_ALIAS_MAX) {
            closeConn(conn, true, kTopicAliasInvalid);
            return;
        }
        uint32_t &a = c.alias[prop.value - 1];
        uint16_t &alen = c.aliasLen[prop.value - 1];
        if (topic.len) {
            if (topic.len > TINYMQ_MAX_TOPIC_LEN || !validTopicName(topic)) {
                closeConn(conn, true, kTopicNameInvalid);
                return;
            }
            pool_.free(a);
            a = storeString(topic);
            alen = (uint16_t)topic.len;
            if (a == Pool::kNull) {
                closeConn(conn, true, kServerBusy);
                return;
            }
        } else if (a == Pool::kNull) {
            closeConn(conn, true, kProtocolError);
            return;
        } else {
            topic = string(a, alen);
        }
#else
        closeConn(conn, true, kTopicAliasInvalid);
        return;
#endif
    }
    if (!validTopicName(topic)) {
        closeConn(conn, true, v5 ? kTopicNameInvalid : kMalformedPacket);
        return;
    }
    if (v5 && p.retain && !retainAvailable_) {
        closeConn(conn, true, kRetainNotSupported);
        return;
    }
    Session &ss = sessions_[c.session];
    if (p.qos == 2) {
        if (qos2Known(ss, p.packetId)) {
            sendAck(conn, PacketType::Pubrec, p.packetId);  // a resend: delivered already
            return;
        }
        if (ss.qos2InCount >= TINYMQ_BROKER_MAX_QOS2_IN) {
            closeConn(conn, true, kReceiveMaximumExceeded);
            return;
        }
    }
    uint8_t reason = kSuccess;
    if (v5 && findProp(p.props, kPayloadFormatIndicator, prop) && prop.value == 1 && !validUtf8(p.payload.data, p.payload.len))
        reason = kPayloadFormatInvalid;
    else if (topic.len > TINYMQ_MAX_TOPIC_LEN || (topic.len >= 4 && memcmp(topic.data, "$SYS", 4) == 0))
        reason = kSuccess;  // accepted and dropped
    else if (!allowed(c.session, topic, false))
        reason = kNotAuthorized;
    else {
        uint32_t expiry = 0;
        if (v5 && findProp(p.props, kMessageExpiryInterval, prop))
            expiry = seconds_ + prop.value;  // seconds_ starts at 1: 0 stays "no expiry"
        uint8_t keep[256];
        Bytes props;
        if (v5 && p.props.len) {
            PropWriter w(keep, sizeof(keep));
            w.copy(p.props, kNotKept, sizeof(kNotKept));
            props = w.ok() ? w.result() : Bytes();
        }
        if (!deliver(c.session, topic, p.payload, p.qos, p.retain, props, expiry))
            reason = kNoMatchingSubscribers;
    }
    if (reason == kPayloadFormatInvalid && p.qos == 0) {
        closeConn(conn, true, reason);
        return;
    }
    if (p.qos == 2 && reason < 0x80)
        ss.qos2In[ss.qos2InCount++] = p.packetId;
    if (p.qos == 1)
        sendAck(conn, PacketType::Puback, p.packetId, reason);
    else if (p.qos == 2)
        sendAck(conn, PacketType::Pubrec, p.packetId, reason);
}

void Broker::handleAck(int conn, const FixedHeader &h, const uint8_t *body, size_t len)
{
    Conn &c = conns_[conn];
    Ack a;
    uint8_t r = decodeAck(h, body, len, c.version, a);
    if (r == kSuccess && c.version == kMqtt5)
        r = checkProps(a.props, kInAck);
    if (r != kSuccess) {
        closeConn(conn, true, r);
        return;
    }
    int s = c.session;
    Session &ss = sessions_[s];
    if (h.type == PacketType::Pubrel) {
        bool found = false;
        for (uint8_t i = 0; i < ss.qos2InCount; ++i)
            if (ss.qos2In[i] == a.packetId) {
                ss.qos2In[i] = ss.qos2In[--ss.qos2InCount];
                found = true;
                break;
            }
        sendAck(conn, PacketType::Pubcomp, a.packetId, found ? kSuccess : kPacketIdentifierNotFound);
        return;
    }
    uint8_t want = h.type == PacketType::Puback ? kWaitPuback : h.type == PacketType::Pubrec ? kWaitPubrec : kWaitPubcomp;
    bool found = false;
    for (int i = 0; i < ss.queued; ++i) {
        Entry &e = ss.queue[i];
        if (e.state != want || e.packetId != a.packetId)
            continue;
        found = true;
        if (h.type == PacketType::Pubrec && a.reason < 0x80) {
            store_.release(e.msg);
            e.msg = Pool::kNull;
            e.state = kWaitPubcomp;
        } else {
            // done (PUBACK, PUBCOMP, or a PUBREC refusing it)
            removeEntry(ss, i);
            --ss.inflight;
            flush(s);
        }
        break;
    }
    if (h.type == PacketType::Pubrec && (a.reason < 0x80 || c.version != kMqtt5))
        sendAck(conn, PacketType::Pubrel, a.packetId, found || c.version != kMqtt5 ? kSuccess : kPacketIdentifierNotFound);
}

void Broker::handleSubscribe(int conn, const uint8_t *body, size_t len)
{
    Conn &c = conns_[conn];
    bool v5 = c.version == kMqtt5;
    Subscribe sp;
    uint8_t r = decodeSubscribe(body, len, c.version, sp);
    if (r == kSuccess && v5)
        r = checkProps(sp.props, kInSubscribe);
    if (r != kSuccess) {
        closeConn(conn, true, r);
        return;
    }
    Property prop;
    uint32_t subId = v5 && findProp(sp.props, kSubscriptionIdentifier, prop) ? prop.value : 0;

    // the reason codes are collected at the end of tx_; encoding moves them
    // forward into the SUBACK (Writer uses memmove)
    size_t count = 0;
    size_t pos = 0;
    Bytes filter;
    SubOptions opts;
    while (nextSubscription(sp, pos, filter, opts))
        ++count;
    if (count + 16 > sizeof(tx_) || count > 64) {
        closeConn(conn, true, kQuotaExceeded);
        return;
    }
    bool sendOld[64];
    uint8_t *codes = tx_ + sizeof(tx_) - count;
    pos = 0;
    for (size_t i = 0; nextSubscription(sp, pos, filter, opts); ++i) {
        sendOld[i] = false;
        Bytes group, inner;
        bool shared = parseSharedFilter(filter, group, inner);
        if (shared && v5 && opts.noLocal) {
            closeConn(conn, true, kProtocolError);  // No Local on a shared subscription
            return;
        }
        if (!validTopicFilter(filter) || filter.len > TINYMQ_MAX_TOPIC_LEN || (shared && group.len == 0) || (shared && !validTopicFilter(inner))) {
            codes[i] = v5 ? kTopicFilterInvalid : 0x80;
            continue;
        }
        if (shared && !TINYMQ_ENABLE_SHARED_SUB) {
            codes[i] = v5 ? kSharedSubscriptionsNotSupported : 0x80;
            continue;
        }
        if (!allowed(c.session, filter, true)) {
            codes[i] = v5 ? kNotAuthorized : 0x80;
            continue;
        }
        uint8_t granted = opts.qos < maxQos_ ? opts.qos : maxQos_;
        if (!v5)
            opts = SubOptions();
        opts.qos = granted;
        bool replaced;
        if (addSubscription(c.session, filter, granted, opts.encode(), subId, replaced) < 0) {
            codes[i] = v5 ? kQuotaExceeded : 0x80;
            continue;
        }
        codes[i] = granted;
        sendOld[i] = !shared && (opts.retainHandling == 0 || (opts.retainHandling == 1 && !replaced));
    }
    SubAck a;
    a.packetId = sp.packetId;
    a.reasons = Bytes(codes, (uint32_t)count);
    size_t n = encodeSubAck(tx_, sizeof(tx_), c.version, PacketType::Suback, a);
    // the codes were overwritten by the SUBACK; read them back from it
    uint8_t granted[64];
    memcpy(granted, tx_ + n - count, count);
    if (!send(conn, tx_, n))
        return;
    // retained messages for the subscriptions, after the SUBACK
    pos = 0;
    for (size_t i = 0; i < count && nextSubscription(sp, pos, filter, opts); ++i)
        if (sendOld[i])
            sendRetained(c.session, filter, granted[i], subId);
}

void Broker::handleUnsubscribe(int conn, const uint8_t *body, size_t len)
{
    Conn &c = conns_[conn];
    Unsubscribe u;
    uint8_t r = decodeUnsubscribe(body, len, c.version, u);
    if (r == kSuccess && c.version == kMqtt5)
        r = checkProps(u.props, kInUnsubscribe);
    if (r != kSuccess) {
        closeConn(conn, true, r);
        return;
    }
    size_t count = 0;
    size_t pos = 0;
    Bytes filter;
    while (nextUnsubscription(u, pos, filter))
        ++count;
    if (count + 16 > sizeof(tx_)) {
        closeConn(conn, true, kQuotaExceeded);
        return;
    }
    uint8_t *codes = tx_ + sizeof(tx_) - count;
    pos = 0;
    for (size_t i = 0; nextUnsubscription(u, pos, filter); ++i)
        codes[i] = removeSubscription(c.session, filter) ? kSuccess : kNoSubscriptionExisted;
    SubAck a;
    a.packetId = u.packetId;
    a.reasons = Bytes(codes, (uint32_t)count);
    size_t n = encodeSubAck(tx_, sizeof(tx_), c.version, PacketType::Unsuback, a);
    send(conn, tx_, n);
}

void Broker::handleDisconnect(int conn, const FixedHeader &h, const uint8_t *body, size_t len)
{
    Conn &c = conns_[conn];
    Disconnect d;
    uint8_t r = decodeDisconnect(h, body, len, c.version, d);
    if (r == kSuccess && c.version == kMqtt5)
        r = checkProps(d.props, kInDisconnect);
    if (r != kSuccess) {
        closeConn(conn, true, r);
        return;
    }
    Session &ss = sessions_[c.session];
    Property p;
    if (c.version == kMqtt5 && findProp(d.props, kSessionExpiryInterval, p)) {
        if (ss.expiry == 0 && p.value != 0) {
            closeConn(conn, true, kProtocolError);  // a session that ends cannot be kept now
            return;
        }
        ss.expiry = sessionExpiry_ && p.value > sessionExpiry_ ? sessionExpiry_ : p.value;
    }
    // a normal disconnect discards the will; 5.0 can ask for it
    closeConn(conn, d.reason == kDisconnectWithWill);
}

void Broker::closeConn(int conn, bool publishWillNow, uint8_t reason)
{
    Conn &c = conns_[conn];
    if (c.state == kFree)
        return;
    if (reason && c.version == kMqtt5 && c.state == kActive && !c.broken) {
        Disconnect d;
        d.reason = reason;
        size_t n = encodeDisconnect(tx_, sizeof(tx_), kMqtt5, PacketType::Disconnect, d);
        c.link->write(tx_, n);
    }
    uint16_t s = c.session;
    Link *link = c.link;
    c.state = kFree;
    c.session = kNone;
    c.rxLen = 0;
    c.link = nullptr;
    c.broken = false;
#if TINYMQ_BROKER_TOPIC_ALIAS_MAX > 0
    for (uint32_t &a : c.alias) {
        pool_.free(a);
        a = Pool::kNull;
    }
#endif
    link->close();
    if (s == kNone)
        return;
    Session &ss = sessions_[s];
    ss.conn = kNone;
    ss.lastSeen = seconds_;
    if (ss.will != Pool::kNull) {
        if (!publishWillNow) {
            store_.release(ss.will);
            ss.will = Pool::kNull;
        } else if (ss.willDelay && ss.expiry) {
            // published when the delay or the session ends, unless the client comes back
            ss.willPending = true;
            ss.willAt = seconds_ + (ss.willDelay < ss.expiry ? ss.willDelay : ss.expiry);
        } else {
            ss.willPending = true;
            publishWill(s);
        }
    }
    if (onClient_)
        onClient_(string(ss.clientId, ss.clientIdLen), false, onClientCtx_);
    detachSession(s);
}

void Broker::publishWill(int s)
{
    Session &ss = sessions_[s];
    if (!ss.willPending || ss.will == Pool::kNull)
        return;
    uint32_t will = ss.will;
    ss.will = Pool::kNull;
    ss.willPending = false;
    uint32_t interval = store_.expiry(will);  // kept as an interval until now
    uint32_t msg = will;
    if (interval) {
        msg = store_.create(store_.topic(will), store_.props(will), store_.payload(will), store_.qos(will), store_.retain(will), seconds_ + interval);
        store_.release(will);
        if (msg == Pool::kNull) {
            ++dropped_;
            return;
        }
    }
    route((uint16_t)s, msg);
    store_.release(msg);
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

bool Broker::sendAck(int conn, PacketType type, uint16_t packetId, uint8_t reason)
{
    Ack a;
    a.packetId = packetId;
    a.reason = reason;
    size_t n = encodeAck(tx_, sizeof(tx_), conns_[conn].version, type, a);
    return send(conn, tx_, n);
}

bool Broker::sendConnack(int conn, uint8_t version, uint8_t reason, bool sessionPresent, Bytes props)
{
    Connack a;
    a.reason = reason;
    a.sessionPresent = sessionPresent;
    a.props = props;
    size_t n = encodeConnack(tx_, sizeof(tx_), version, a);
    return send(conn, tx_, n);
}

bool Broker::expired(uint32_t msg) const
{
    uint32_t at = store_.expiry(msg);
    return at && (int32_t)(seconds_ - at) >= 0;
}

int Broker::sendPublish(int conn, const Entry &e, bool dup)
{
    Conn &c = conns_[conn];
    uint32_t msg = e.msg;
    Publish p;
    p.topic = store_.topic(msg);
    p.payload = store_.payload(msg);
    p.qos = e.qos;
    p.retain = e.retain;
    p.dup = dup;
    p.packetId = e.packetId;
    uint8_t extra[16];
    if (c.version == kMqtt5) {
        p.props = store_.props(msg);
        PropWriter w(extra, sizeof(extra));
        uint32_t at = store_.expiry(msg);
        if (at)
            w.u32(kMessageExpiryInterval, (int32_t)(at - seconds_) > 0 ? at - seconds_ : 0);
        if (e.subId)
            w.varint(kSubscriptionIdentifier, e.subId);
        p.extraProps = w.result();
    }
    size_t head = encodePublishHead(tx_, sizeof(tx_), c.version, p);
    if (head == 0 || head + p.payload.len > c.maxPacket)
        return 1;  // the client could not take it: as if delivered
    size_t len = p.payload.len;
    if (head + len <= sizeof(tx_)) {
        if (len)
            memcpy(tx_ + head, p.payload.data, len);
        return send(conn, tx_, head + len) ? 0 : -1;
    }
    return send(conn, tx_, head) && send(conn, p.payload.data, len) ? 0 : -1;
}

uint8_t Broker::inflightLimit(int conn) const
{
    uint16_t m = conns_[conn].receiveMax;
    return m < TINYMQ_BROKER_MAX_INFLIGHT ? (uint8_t)m : (uint8_t)TINYMQ_BROKER_MAX_INFLIGHT;
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
    (void)now;
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
            if (ss.conn == kNone && (pick < 0 || seconds_ - ss.lastSeen > oldest)) {
                pick = s;
                oldest = seconds_ - ss.lastSeen;
            }
        }
        if (pick < 0)
            return -1;
        publishWill(pick);
        destroySession(pick);
    }
    Session &ss = sessions_[pick];
    ss.used = true;
    ss.version = kMqtt311;
    ss.conn = kNone;
    ss.clientId = Pool::kNull;
    ss.clientIdLen = 0;
    ss.username = Pool::kNull;
    ss.usernameLen = 0;
    ss.expiry = 0;
    ss.will = Pool::kNull;
    ss.willDelay = 0;
    ss.willPending = false;
    ss.lastSeen = seconds_;
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
            pool_.free(sub.group);
            sub.session = kNone;
        }
    for (int i = 0; i < ss.queued; ++i)
        store_.release(ss.queue[i].msg);
    ss.queued = 0;
    ss.inflight = 0;
    ss.qos2InCount = 0;
    store_.release(ss.will);
    ss.will = Pool::kNull;
    ss.willPending = false;
    pool_.free(ss.clientId);
    pool_.free(ss.username);
    ss.used = false;
}

void Broker::detachSession(int s)
{
    Session &ss = sessions_[s];
    if (ss.expiry == 0) {
        publishWill(s);
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

void Broker::enqueue(int s, uint32_t msg, uint8_t qos, bool retain, uint32_t subId)
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
    e.subId = subId;
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
    uint8_t limit = inflightLimit(conn);
    for (int i = 0; i < ss.queued;) {
        Entry &e = ss.queue[i];
        if (e.state != kQueued) {
            ++i;
            continue;
        }
        if (expired(e.msg)) {
            removeEntry(ss, i);  // not started in time: dropped
            continue;
        }
        if (e.qos > 0 && ss.inflight >= limit)
            break;  // later messages wait too, to keep the order
        size_t need = 9 + store_.topic(e.msg).len + store_.props(e.msg).len + 16 + store_.payload(e.msg).len;
        if (c.link->writable() < need) {
            pendingFlush_ = true;
            break;
        }
        if (e.qos == 0) {
            int r = sendPublish(conn, e, false);
            removeEntry(ss, i);
            if (r < 0)
                break;
            continue;
        }
        e.packetId = nextPacketId(ss);
        int r = sendPublish(conn, e, false);
        if (r == 1) {
            removeEntry(ss, i);  // too large for this client
            continue;
        }
        e.state = e.qos == 1 ? kWaitPuback : kWaitPubrec;
        ++ss.inflight;
        if (r < 0)
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
            sendPublish(conn, e, true);
    }
}

// ---- messages

bool Broker::deliver(uint16_t from, Bytes topic, Bytes payload, uint8_t qos, bool retain, Bytes props, uint32_t expiry)
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
            return false;
    }
    uint32_t msg = store_.create(topic, props, payload, qos, retain, expiry);
    if (msg == Pool::kNull) {
        ++dropped_;
        if (retain && retainAvailable_ && payload.len == 0)
            storeRetained(Pool::kNull, topic);  // deleting needs no room
        return true;
    }
    bool any = route(from, msg);
    store_.release(msg);
    return any;
}

bool Broker::route(uint16_t from, uint32_t msg)
{
    Bytes topic = store_.topic(msg);
    uint8_t qos = store_.qos(msg);
    bool msgRetain = store_.retain(msg);
    if (msgRetain && retainAvailable_)
        storeRetained(msg, topic);
    if (expired(msg))
        return false;

    // 3.1.1 sessions: one delivery at the highest QoS of their matching
    // subscriptions. 5.0 sessions: one per subscription (each with its own
    // identifier and options). Shared subscriptions: one member per group.
    uint8_t best[TINYMQ_BROKER_MAX_SESSIONS];
    memset(best, 0xFF, sizeof(best));
    bool any = false;
    int local = -1;
    for (int i = 0; i < TINYMQ_BROKER_MAX_SUBSCRIPTIONS; ++i) {
        const Sub &sub = subs_[i];
        if (sub.session == kNone || !topicMatches(string(sub.filter, sub.filterLen), topic))
            continue;
        any = true;
        if (sub.session == kLocal) {
            if (sub.qos > local)
                local = sub.qos;
            continue;
        }
        SubOptions o = SubOptions::decode(sub.options);
        uint8_t q = qos < sub.qos ? qos : sub.qos;
        if (sub.group != Pool::kNull) {
            // the first matching member of a group picks one member for all
            Bytes g = string(sub.group, sub.groupLen);
            Bytes f = string(sub.filter, sub.filterLen);
            bool done = false;
            for (int j = 0; j < i && !done; ++j)
                done = subs_[j].session != kNone && subs_[j].session != kLocal && subs_[j].group != Pool::kNull &&
                       string(subs_[j].group, subs_[j].groupLen).equals(g) && string(subs_[j].filter, subs_[j].filterLen).equals(f);
            if (done)
                continue;
            int members[16];
            int count = 0, online = 0;
            for (int j = i; j < TINYMQ_BROKER_MAX_SUBSCRIPTIONS && count < 16; ++j)
                if (subs_[j].session != kNone && subs_[j].session != kLocal && subs_[j].group != Pool::kNull &&
                    string(subs_[j].group, subs_[j].groupLen).equals(g) && string(subs_[j].filter, subs_[j].filterLen).equals(f)) {
                    // online members first
                    if (sessions_[subs_[j].session].conn != kNone) {
                        for (int k = count; k > online; --k)
                            members[k] = members[k - 1];
                        members[online++] = j;
                    } else {
                        members[count] = j;
                    }
                    ++count;
                }
            const Sub &m = subs_[members[online ? sharedNext_ % online : sharedNext_ % count]];
            ++sharedNext_;
            enqueue(m.session, msg, qos < m.qos ? qos : m.qos, false, m.subId);
            flush(m.session);
            continue;
        }
        if (sessions_[sub.session].version == kMqtt5) {
            if (o.noLocal && sub.session == from)
                continue;
            enqueue(sub.session, msg, q, o.retainAsPublished && msgRetain, sub.subId);
            flush(sub.session);
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
    return any;
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

void Broker::sendRetained(uint16_t s, Bytes filter, uint8_t grantedQos, uint32_t subId)
{
    for (uint32_t &r : retained_) {
        if (r == Pool::kNull)
            continue;
        if (expired(r)) {
            store_.release(r);
            r = Pool::kNull;
            continue;
        }
        if (!topicMatches(filter, store_.topic(r)))
            continue;
        uint8_t qos = store_.qos(r) < grantedQos ? store_.qos(r) : grantedQos;
        if (s == kLocal)
            deliverLocal(r, qos, true);
        else
            enqueue(s, r, qos, true, subId);
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
    m.props = store_.props(msg);
    onLocal_(m, onLocalCtx_);
}

int Broker::findSubscription(uint16_t s, Bytes filter) const
{
    Bytes group, inner;
    bool shared = parseSharedFilter(filter, group, inner);
    Bytes f = shared ? inner : filter;
    for (int i = 0; i < TINYMQ_BROKER_MAX_SUBSCRIPTIONS; ++i) {
        const Sub &sub = subs_[i];
        if (sub.session != s || !string(sub.filter, sub.filterLen).equals(f))
            continue;
        if (shared ? (sub.group != Pool::kNull && string(sub.group, sub.groupLen).equals(group)) : sub.group == Pool::kNull)
            return i;
    }
    return -1;
}

int Broker::addSubscription(uint16_t s, Bytes filter, uint8_t qos, uint8_t options, uint32_t subId, bool &replaced)
{
    int i = findSubscription(s, filter);
    replaced = i >= 0;
    if (replaced) {
        subs_[i].qos = qos;
        subs_[i].options = options;
        subs_[i].subId = subId;
        return i;
    }
    for (i = 0; i < TINYMQ_BROKER_MAX_SUBSCRIPTIONS && subs_[i].session != kNone; ++i) {
    }
    if (i == TINYMQ_BROKER_MAX_SUBSCRIPTIONS)
        return -1;
    Bytes group, inner;
    bool shared = parseSharedFilter(filter, group, inner);
    uint32_t f = storeString(shared ? inner : filter);
    uint32_t g = shared ? storeString(group) : Pool::kNull;
    if (f == Pool::kNull || (shared && g == Pool::kNull)) {
        pool_.free(f);
        pool_.free(g);
        return -1;
    }
    Sub &sub = subs_[i];
    sub.session = s;
    sub.qos = qos;
    sub.options = options;
    sub.subId = subId;
    sub.filter = f;
    sub.filterLen = (uint16_t)(shared ? inner.len : filter.len);
    sub.group = g;
    sub.groupLen = (uint16_t)group.len;
    return i;
}

bool Broker::removeSubscription(uint16_t s, Bytes filter)
{
    int i = findSubscription(s, filter);
    if (i < 0)
        return false;
    pool_.free(subs_[i].filter);
    pool_.free(subs_[i].group);
    subs_[i].session = kNone;
    return true;
}

bool Broker::allowed(int s, Bytes topic, bool subscribe)
{
    if (!onAcl_)
        return true;
    const Session &ss = sessions_[s];
    return onAcl_(string(ss.clientId, ss.clientIdLen), string(ss.username, ss.usernameLen), topic, subscribe, onAclCtx_);
}

}  // namespace tinymq
