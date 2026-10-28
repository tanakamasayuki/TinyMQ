#include "mq_client.h"

#include "mq_topic.h"

namespace tinymq {

ClientEngine::ClientEngine() : store_(pool_)
{
    pool_.begin(arena_, sizeof(arena_));
}

void ClientEngine::setCredentials(const char *username, const char *password)
{
    connect_.hasUsername = username != nullptr;
    connect_.username = Bytes(username);
    connect_.hasPassword = password != nullptr;
    connect_.password = Bytes(password);
}

void ClientEngine::setWill(const char *topic, const uint8_t *payload, size_t len, uint8_t qos, bool retain)
{
    connect_.hasWill = true;
    connect_.willTopic = Bytes(topic);
    connect_.willPayload = Bytes(payload, (uint32_t)len);
    connect_.willQos = qos > 2 ? 2 : qos;
    connect_.willRetain = retain;
}

// ---- transport side

bool ClientEngine::start(Link &link, uint32_t now)
{
    link_ = &link;
    now_ = now;
    rxLen_ = 0;
    skip_ = 0;
    pingOutstanding_ = false;
    lastReason_ = kSuccess;
    if (connect_.cleanStart)
        clearSession();
    keepAlive_ = connect_.keepAlive;
    assigned_[0] = 0;
    serverReceiveMax_ = 0xFFFF;
    serverMaxPacket_ = 0xFFFFFFFF;
    serverMaxQos_ = 2;
    serverRetain_ = true;
    connect_.props = Bytes();
    if (connect_.version == kMqtt5) {
        PropWriter w(connectProps_, sizeof(connectProps_));
        if (sessionExpiry_)
            w.u32(kSessionExpiryInterval, sessionExpiry_);
        w.u16(kReceiveMaximum, TINYMQ_CLIENT_MAX_QOS2_IN);
        w.u32(kMaximumPacketSize, sizeof(rx_));
        w.copy(extraConnectProps_);
        if (!w.ok()) {
            state_ = State::WaitConnack;
            close(kPacketTooLarge);
            return false;
        }
        connect_.props = w.result();
    }
    size_t n = encodeConnect(tx_, sizeof(tx_), connect_);
    state_ = State::WaitConnack;
    connectSentAt_ = now;
    if (n == 0) {
        close(kPacketTooLarge);
        return false;
    }
    return send(tx_, n);
}

uint8_t *ClientEngine::rxBuffer(size_t &room)
{
    room = sizeof(rx_) - rxLen_;
    return rx_ + rxLen_;
}

void ClientEngine::rxCommit(size_t n, uint32_t now)
{
    now_ = now;
    rxLen_ += n;
    size_t pos = 0;
    while (state_ != State::Disconnected && pos < rxLen_) {
        size_t avail = rxLen_ - pos;
        if (skip_) {
            size_t k = skip_ < avail ? skip_ : avail;
            skip_ -= (uint32_t)k;
            pos += k;
            continue;
        }
        FixedHeader h;
        HeaderResult r = parseFixedHeader(rx_ + pos, avail, h);
        if (r == HeaderResult::NeedMore)
            break;
        if (r == HeaderResult::Malformed) {
            fail(kMalformedPacket);
            return;
        }
        size_t total = h.size + (size_t)h.remaining;
        if (total <= avail) {
            handlePacket(h, rx_ + pos + h.size);
            pos += total;
            continue;
        }
        if (total <= sizeof(rx_))
            break;  // fits once the rest arrives
        // Larger than the buffer: only a PUBLISH can be skipped, once the
        // buffer is as full as it gets.
        if (h.type != PacketType::Publish) {
            fail(kPacketTooLarge);
            return;
        }
        if (pos > 0 || rxLen_ < sizeof(rx_))
            break;
        handleOversizePublish(h, rx_ + h.size, avail - h.size);
        skip_ = (uint32_t)(total - avail);
        pos = rxLen_;
    }
    if (state_ == State::Disconnected) {
        rxLen_ = 0;
        return;
    }
    if (pos) {
        memmove(rx_, rx_ + pos, rxLen_ - pos);
        rxLen_ -= pos;
    }
}

void ClientEngine::poll(uint32_t now)
{
    now_ = now;
    if (state_ == State::WaitConnack) {
        if (now - connectSentAt_ >= connackTimeout_)
            close(kKeepAliveTimeout);
        return;
    }
    if (state_ != State::Connected || keepAlive_ == 0)
        return;
    uint32_t interval = (uint32_t)keepAlive_ * 1000;
    if (pingOutstanding_) {
        if (now - pingSentAt_ >= interval)
            close(kKeepAliveTimeout);
    } else if (now - lastTx_ >= interval) {
        size_t n = encodePingreq(tx_, sizeof(tx_));
        if (send(tx_, n)) {
            pingOutstanding_ = true;
            pingSentAt_ = now;
        }
    }
}

void ClientEngine::linkLost()
{
    if (state_ != State::Disconnected)
        close(kUnspecifiedError);
}

// ---- application side

int ClientEngine::publish(const char *topic, const uint8_t *payload, size_t len, uint8_t qos, bool retain, Bytes props)
{
    if (state_ != State::Connected || qos > serverMaxQos_ || (retain && !serverRetain_))
        return -1;
    Bytes t(topic);
    if (!validTopicName(t) || !validUtf8(t.data, t.len) || t.len > 0xFFFF)
        return -1;
    if (connect_.version != kMqtt5)
        props = Bytes();
    Property pr;
    if (props.len && (checkProps(props, kInPublish) != kSuccess || findProp(props, kTopicAlias, pr) || findProp(props, kSubscriptionIdentifier, pr)))
        return -1;
    // the packet must fit what the server takes
    size_t size = 1 + 4 + 2 + t.len + (qos ? 2 : 0) + (connect_.version == kMqtt5 ? varintSize(props.len) + props.len : 0) + len;
    if (size > serverMaxPacket_)
        return -1;
    Bytes p(payload, (uint32_t)len);
    if (qos == 0) {
        Publish pub;
        pub.topic = t;
        pub.payload = p;
        pub.retain = retain;
        pub.props = props;
        size_t head = encodePublishHead(tx_, sizeof(tx_), connect_.version, pub);
        if (head == 0)
            return -1;
        bool ok;
        if (head + len <= sizeof(tx_)) {
            if (len)
                memcpy(tx_ + head, payload, len);
            ok = send(tx_, head + len);
        } else {
            ok = send(tx_, head) && send(payload, len);
        }
        return ok ? 0 : -1;
    }
    if (inflightCount_ >= TINYMQ_CLIENT_MAX_INFLIGHT || inflightCount_ >= serverReceiveMax_)
        return -1;
    uint32_t msg = store_.create(t, props, p, qos, retain);
    if (msg == Pool::kNull)
        return -1;
    uint16_t id = nextPacketId();
    Inflight &f = inflight_[inflightCount_++];
    f.packetId = id;
    f.state = qos == 1 ? WaitPuback : WaitPubrec;
    f.msg = msg;
    if (!sendPublish(msg, id, false))
        return -1;  // kept for the resend after reconnecting
    return id;
}

int ClientEngine::subscribe(const char *filter, uint8_t qos)
{
    SubOptions o;
    o.qos = qos;
    return subscribe(filter, o, 0);
}

int ClientEngine::subscribe(const char *filter, SubOptions o, uint32_t subscriptionId)
{
    if (state_ != State::Connected || o.qos > 2 || o.retainHandling > 2 || subscriptionId > 268435455)
        return -1;
    if (connect_.version != kMqtt5) {
        uint8_t qos = o.qos;
        o = SubOptions();
        o.qos = qos;
        subscriptionId = 0;
    }
    Bytes f(filter);
    if (!validTopicFilter(f) || !validUtf8(f.data, f.len))
        return -1;
    // the entry is built at the end of tx_, then the packet in front of it
    size_t entryCap = f.len + 3;
    if (entryCap + 16 > sizeof(tx_))
        return -1;
    uint8_t *entry = tx_ + sizeof(tx_) - entryCap;
    size_t entryLen = appendSubscription(entry, entryCap, 0, f, o);
    Subscribe s;
    s.packetId = nextPacketId();
    uint8_t props[8];
    if (subscriptionId) {
        PropWriter w(props, sizeof(props));
        w.varint(kSubscriptionIdentifier, subscriptionId);
        s.props = w.result();
    }
    // encoding moves `entries` forward within tx_ (Writer uses memmove)
    s.entries = Bytes(entry, (uint32_t)entryLen);
    size_t n = encodeSubscribe(tx_, sizeof(tx_), connect_.version, s);
    if (n == 0 || !send(tx_, n))
        return -1;
    return s.packetId;
}

int ClientEngine::unsubscribe(const char *filter)
{
    if (state_ != State::Connected)
        return -1;
    Bytes f(filter);
    if (!validTopicFilter(f) || !validUtf8(f.data, f.len))
        return -1;
    size_t entryCap = f.len + 2;
    if (entryCap + 16 > sizeof(tx_))
        return -1;
    uint8_t *entry = tx_ + sizeof(tx_) - entryCap;
    size_t entryLen = appendUnsubscription(entry, entryCap, 0, f);
    Unsubscribe u;
    u.packetId = nextPacketId();
    u.entries = Bytes(entry, (uint32_t)entryLen);
    size_t n = encodeUnsubscribe(tx_, sizeof(tx_), connect_.version, u);
    if (n == 0 || !send(tx_, n))
        return -1;
    return u.packetId;
}

void ClientEngine::disconnect(uint8_t reason)
{
    if (state_ == State::Disconnected)
        return;
    if (state_ == State::Connected) {
        Disconnect d;
        d.reason = reason;
        size_t n = encodeDisconnect(tx_, sizeof(tx_), connect_.version, PacketType::Disconnect, d);
        link_->write(tx_, n);
    }
    close(kSuccess);
}

// ---- internals

bool ClientEngine::send(const uint8_t *data, size_t len)
{
    if (!link_ || state_ == State::Disconnected)
        return false;
    if (link_->write(data, len) != len) {
        close(kUnspecifiedError);
        return false;
    }
    lastTx_ = now_;
    return true;
}

bool ClientEngine::sendPublish(uint32_t msg, uint16_t packetId, bool dup)
{
    Publish pub;
    pub.topic = store_.topic(msg);
    pub.props = store_.props(msg);
    pub.payload = store_.payload(msg);
    pub.qos = store_.qos(msg);
    pub.retain = store_.retain(msg);
    pub.dup = dup;
    pub.packetId = packetId;
    size_t head = encodePublishHead(tx_, sizeof(tx_), connect_.version, pub);
    if (head == 0)
        return false;
    size_t len = pub.payload.len;
    if (head + len <= sizeof(tx_)) {
        if (len)
            memcpy(tx_ + head, pub.payload.data, len);
        return send(tx_, head + len);
    }
    return send(tx_, head) && send(pub.payload.data, len);
}

bool ClientEngine::sendAck(PacketType type, uint16_t packetId)
{
    Ack a;
    a.packetId = packetId;
    size_t n = encodeAck(tx_, sizeof(tx_), connect_.version, type, a);
    return send(tx_, n);
}

void ClientEngine::close(uint8_t reason)
{
    if (state_ == State::Disconnected)
        return;
    state_ = State::Disconnected;
    lastReason_ = reason;
    pingOutstanding_ = false;
    if (link_)
        link_->close();
    if (onClosed_)
        onClosed_(reason, onClosedCtx_);
}

void ClientEngine::fail(uint8_t reason)
{
    if (state_ == State::Connected && connect_.version == kMqtt5) {
        Disconnect d;
        d.reason = reason;
        size_t n = encodeDisconnect(tx_, sizeof(tx_), kMqtt5, PacketType::Disconnect, d);
        link_->write(tx_, n);
    }
    close(reason);
}

void ClientEngine::clearSession()
{
    for (size_t i = 0; i < inflightCount_; ++i)
        store_.release(inflight_[i].msg);
    inflightCount_ = 0;
    qos2InCount_ = 0;
}

bool ClientEngine::packetIdInUse(uint16_t id) const
{
    for (size_t i = 0; i < inflightCount_; ++i)
        if (inflight_[i].packetId == id)
            return true;
    return false;
}

uint16_t ClientEngine::nextPacketId()
{
    do {
        if (++lastPacketId_ == 0)
            lastPacketId_ = 1;
    } while (packetIdInUse(lastPacketId_));
    return lastPacketId_;
}

int ClientEngine::findInflight(uint16_t id, uint8_t state) const
{
    for (size_t i = 0; i < inflightCount_; ++i)
        if (inflight_[i].packetId == id && inflight_[i].state == state)
            return (int)i;
    return -1;
}

void ClientEngine::removeInflight(int index)
{
    store_.release(inflight_[index].msg);
    for (size_t i = (size_t)index + 1; i < inflightCount_; ++i)
        inflight_[i - 1] = inflight_[i];
    --inflightCount_;
}

bool ClientEngine::knowsQos2(uint16_t id) const
{
    for (size_t i = 0; i < qos2InCount_; ++i)
        if (qos2In_[i] == id)
            return true;
    return false;
}

bool ClientEngine::rememberQos2(uint16_t id)
{
    if (knowsQos2(id))
        return true;
    if (qos2InCount_ >= TINYMQ_CLIENT_MAX_QOS2_IN)
        return false;
    qos2In_[qos2InCount_++] = id;
    return true;
}

bool ClientEngine::forgetQos2(uint16_t id)
{
    for (size_t i = 0; i < qos2InCount_; ++i)
        if (qos2In_[i] == id) {
            qos2In_[i] = qos2In_[--qos2InCount_];
            return true;
        }
    return false;
}

void ClientEngine::handlePacket(const FixedHeader &h, const uint8_t *body)
{
    size_t len = h.remaining;
    if (state_ == State::WaitConnack) {
        if (h.type == PacketType::Connack)
            handleConnack(body, len);
        else
            close(kProtocolError);
        return;
    }
    switch (h.type) {
    case PacketType::Publish:
        handlePublish(h, body, len);
        break;
    case PacketType::Puback:
    case PacketType::Pubrec:
    case PacketType::Pubrel:
    case PacketType::Pubcomp:
        handleAck(h, body, len);
        break;
    case PacketType::Suback:
    case PacketType::Unsuback: {
        SubAck a;
        if (decodeSubAck(h, body, len, connect_.version, a) != kSuccess) {
            fail(kMalformedPacket);
            return;
        }
        if (onSuback_)
            onSuback_(a.packetId, h.type == PacketType::Unsuback, a.reasons.data, a.reasons.len, onSubackCtx_);
        break;
    }
    case PacketType::Pingresp:
        if (len != 0) {
            fail(kMalformedPacket);
            return;
        }
        pingOutstanding_ = false;
        break;
    case PacketType::Disconnect: {
        Disconnect d;
        uint8_t r = decodeDisconnect(h, body, len, connect_.version, d);
        if (r != kSuccess || connect_.version != kMqtt5) {
            close(kProtocolError);  // a server never sends DISCONNECT before 5.0
            return;
        }
        close(d.reason);
        break;
    }
    default:
        fail(kProtocolError);
        break;
    }
}

void ClientEngine::handleConnack(const uint8_t *body, size_t len)
{
    Connack c;
    uint8_t r = decodeConnack(body, len, connect_.version, c);
    if (r == kSuccess && connect_.version == kMqtt5)
        r = checkProps(c.props, kInConnack);
    if (r != kSuccess) {
        close(r);
        return;
    }
    if (c.reason != kSuccess) {
        if (onConnack_)
            onConnack_(c.reason, false, onConnackCtx_);
        close(c.reason);
        return;
    }
    if (connect_.version == kMqtt5) {
        PropReader pr(c.props);
        Property p;
        while (pr.next(p)) {
            switch (p.id) {
            case kAssignedClientIdentifier: {
                size_t n = p.data.len < sizeof(assigned_) - 1 ? p.data.len : sizeof(assigned_) - 1;
                memcpy(assigned_, p.data.data, n);
                assigned_[n] = 0;
                break;
            }
            case kServerKeepAlive:
                keepAlive_ = (uint16_t)p.value;
                break;
            case kReceiveMaximum:
                serverReceiveMax_ = (uint16_t)p.value;
                break;
            case kMaximumPacketSize:
                serverMaxPacket_ = p.value;
                break;
            case kMaximumQos:
                serverMaxQos_ = (uint8_t)p.value;
                break;
            case kRetainAvailable:
                serverRetain_ = p.value != 0;
                break;
            default:
                break;
            }
        }
    }
    state_ = State::Connected;
    if (!c.sessionPresent) {
        clearSession();  // the server has no session: neither do we
    } else {
        // resend what was not acknowledged, in order
        for (size_t i = 0; i < inflightCount_ && state_ == State::Connected; ++i) {
            Inflight &f = inflight_[i];
            if (f.state == WaitPubcomp)
                sendAck(PacketType::Pubrel, f.packetId);
            else
                sendPublish(f.msg, f.packetId, true);
        }
    }
    if (onConnack_ && state_ == State::Connected)
        onConnack_(kSuccess, c.sessionPresent, onConnackCtx_);
}

void ClientEngine::handlePublish(const FixedHeader &h, const uint8_t *body, size_t len)
{
    Publish p;
    uint8_t r = decodePublish(h, body, len, connect_.version, p);
    Property alias;
    if (r == kSuccess && connect_.version == kMqtt5) {
        r = checkProps(p.props, kInPublish);
        if (r == kSuccess && findProp(p.props, kTopicAlias, alias))
            r = kTopicAliasInvalid;  // we said Topic Alias Maximum 0
    }
    if (r == kSuccess && !validTopicName(p.topic))
        r = connect_.version == kMqtt5 ? kTopicNameInvalid : kMalformedPacket;
    if (r != kSuccess) {
        fail(r);
        return;
    }
    bool deliver = !(p.qos == 2 && knowsQos2(p.packetId));
    if (deliver && p.topic.len > TINYMQ_MAX_TOPIC_LEN) {
        ++dropped_;
        deliver = false;
    }
    if (p.qos == 2)
        rememberQos2(p.packetId);  // when full, a resend may be delivered twice
    if (deliver && onMessage_) {
        memcpy(topic_, p.topic.data, p.topic.len);
        topic_[p.topic.len] = 0;
        Message m;
        m.topic = topic_;
        m.topicLen = (uint16_t)p.topic.len;
        m.payload = p.payload.data;
        m.payloadLen = p.payload.len;
        m.qos = p.qos;
        m.retain = p.retain;
        m.dup = p.dup;
        m.props = p.props;
        onMessage_(m, onMessageCtx_);
    }
    if (state_ != State::Connected)
        return;
    if (p.qos == 1)
        sendAck(PacketType::Puback, p.packetId);
    else if (p.qos == 2)
        sendAck(PacketType::Pubrec, p.packetId);
}

void ClientEngine::handleOversizePublish(const FixedHeader &h, const uint8_t *prefix, size_t len)
{
    // decodePublish() on the prefix reads the topic and packet identifier;
    // the payload it sees is cut short and is not used.
    Publish p;
    if (decodePublish(h, prefix, len, connect_.version, p) != kSuccess) {
        close(kPacketTooLarge);
        return;
    }
    ++dropped_;
    if (p.qos == 1)
        sendAck(PacketType::Puback, p.packetId);
    else if (p.qos == 2) {
        rememberQos2(p.packetId);
        sendAck(PacketType::Pubrec, p.packetId);
    }
}

void ClientEngine::handleAck(const FixedHeader &h, const uint8_t *body, size_t len)
{
    Ack a;
    if (decodeAck(h, body, len, connect_.version, a) != kSuccess) {
        fail(kMalformedPacket);
        return;
    }
    int i;
    switch (h.type) {
    case PacketType::Puback:
        i = findInflight(a.packetId, WaitPuback);
        if (i >= 0) {
            removeInflight(i);
            if (onPublished_)
                onPublished_(a.packetId, a.reason, onPublishedCtx_);
        }
        break;
    case PacketType::Pubrec:
        i = findInflight(a.packetId, WaitPubrec);
        if (i >= 0) {
            if (a.reason >= 0x80) {  // 5.0: the server refused it
                removeInflight(i);
                if (onPublished_)
                    onPublished_(a.packetId, a.reason, onPublishedCtx_);
                break;
            }
            store_.release(inflight_[i].msg);
            inflight_[i].msg = Pool::kNull;
            inflight_[i].state = WaitPubcomp;
        }
        sendAck(PacketType::Pubrel, a.packetId);
        break;
    case PacketType::Pubrel:
        forgetQos2(a.packetId);
        sendAck(PacketType::Pubcomp, a.packetId);
        break;
    case PacketType::Pubcomp:
        i = findInflight(a.packetId, WaitPubcomp);
        if (i >= 0) {
            removeInflight(i);
            if (onPublished_)
                onPublished_(a.packetId, a.reason, onPublishedCtx_);
        }
        break;
    default:
        break;
    }
}

}  // namespace tinymq
