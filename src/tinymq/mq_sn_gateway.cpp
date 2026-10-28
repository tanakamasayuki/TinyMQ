#include "mq_sn_gateway.h"

#include "mq_topic.h"

namespace tinymq {

SnGateway::SnGateway()
{
    for (size_t i = 0; i < TINYMQ_SN_MAX_CLIENTS; ++i) {
        clients_[i].gw = this;
        clients_[i].index = (uint8_t)i;
    }
    for (Registration &r : regs_)
        r.client = 0xFF;
    pool_.begin(arena_, sizeof(arena_));
}

void SnGateway::begin(Broker &broker, DatagramSender &out)
{
    broker_ = &broker;
    out_ = &out;
}

bool SnGateway::addPredefinedTopic(uint16_t id, const char *name)
{
    if (predefinedCount_ >= TINYMQ_SN_MAX_PREDEFINED || id == 0 || id == 0xFFFF || !validTopicName(Bytes(name)))
        return false;
    predefined_[predefinedCount_].id = id;
    predefined_[predefinedCount_].name = name;
    ++predefinedCount_;
    return true;
}

void SnGateway::setAdvertise(uint16_t seconds, const Endpoint &to)
{
    advertiseSeconds_ = seconds;
    advertiseTo_ = to;
}

void SnGateway::setRetry(uint32_t ms, uint8_t count)
{
    retryMs_ = ms;
    retryCount_ = count;
}

size_t SnGateway::clients() const
{
    size_t n = 0;
    for (const Client &c : clients_)
        if (c.state != kFree)
            ++n;
    return n;
}

size_t SnGateway::registrations() const
{
    size_t n = 0;
    for (const Registration &r : regs_)
        if (r.client != 0xFF)
            ++n;
    return n;
}

// ---- transport side

void SnGateway::input(const Endpoint &from, const uint8_t *data, size_t len, uint32_t now)
{
    now_ = now;
    if (!broker_)
        return;
    uint8_t type;
    const uint8_t *body;
    size_t bodyLen;
    if (!sn::parseHeader(data, len, type, body, bodyLen))
        return;
    Client *c = findByAddr(from);
    if (c)
        c->lastRx = now;
    bool active = c && (c->state == kActive || c->state == kAwake);
    uint16_t msgId;
    switch (type) {
    case sn::kSearchGw:
        send(from, sn::encodeGwInfo(tx_, sizeof(tx_), gwId_));
        break;
    case sn::kConnect:
        onConnect(from, body, bodyLen, now);
        break;
    case sn::kWillTopic: {
        sn::WillTopic w;
        if (!c || c->state != kWaitWillTopic || !sn::decodeWillTopic(body, bodyLen, w))
            break;
        if (!w.present) {
            startSession(*c, Bytes());
            break;
        }
        if (!validTopicName(w.topic) || w.topic.len > TINYMQ_MAX_TOPIC_LEN || w.flags.qos < 0) {
            sendTo(*c, sn::encodeConnack(tx_, sizeof(tx_), sn::kNotSupported));
            freeClient(*c);
            break;
        }
        pool_.free(c->willTopic);
        c->willTopic = pool_.alloc(w.topic.len);
        if (c->willTopic == Pool::kNull) {
            sendTo(*c, sn::encodeConnack(tx_, sizeof(tx_), sn::kCongestion));
            freeClient(*c);
            break;
        }
        memcpy(pool_.ptr(c->willTopic), w.topic.data, w.topic.len);
        c->willTopicLen = (uint16_t)w.topic.len;
        c->willQos = (uint8_t)w.flags.qos;
        c->willRetain = w.flags.retain;
        c->state = kWaitWillMsg;
        sendTo(*c, sn::encodeEmpty(tx_, sizeof(tx_), sn::kWillMsgReq));
        break;
    }
    case sn::kWillMsg:
        if (c && c->state == kWaitWillMsg)
            startSession(*c, Bytes(body, (uint32_t)bodyLen));
        break;
    case sn::kRegister:
        if (active)
            onRegister(*c, body, bodyLen);
        break;
    case sn::kRegack:
        if (active)
            onRegack(*c, body, bodyLen);
        break;
    case sn::kPublish:
        onPublish(active ? c : nullptr, from, body, bodyLen);
        break;
    case sn::kPuback: {
        sn::TopicAck a;
        if (!c || c->conn < 0 || !sn::decodeTopicAck(body, bodyLen, a))
            break;
        c->retries = 0;
        c->lastProgress = now;
        if (a.rc == sn::kInvalidTopicId) {
            // the client does not know the topic id: register it again and
            // send the message again
            int r = findRegistration(c->index, a.topicId);
            if (r >= 0) {
                pool_.free(regs_[r].name);
                regs_[r].client = 0xFF;
            }
            if (c->state == kActive || c->state == kAwake)
                broker_->resendConn(c->conn);
            break;
        }
        feedAck(*c, PacketType::Puback, a.msgId);
        break;
    }
    case sn::kPubrec:
    case sn::kPubrel:
    case sn::kPubcomp:
        if (!c || c->conn < 0 || !sn::decodeMsgId(body, bodyLen, msgId))
            break;
        if (type != sn::kPubrel) {
            c->retries = 0;
            c->lastProgress = now;
        }
        feedAck(*c, type == sn::kPubrec ? PacketType::Pubrec : type == sn::kPubrel ? PacketType::Pubrel : PacketType::Pubcomp, msgId);
        break;
    case sn::kSubscribe:
    case sn::kUnsubscribe:
        if (active)
            onSubscribe(*c, type, body, bodyLen);
        break;
    case sn::kPingreq:
        onPingreq(c, from, body, bodyLen, now);
        break;
    case sn::kDisconnect:
        if (c)
            onDisconnect(*c, body, bodyLen);
        break;
    case sn::kWillTopicUpd:
    case sn::kWillMsgUpd:
        if (active)
            onWillUpdate(*c, type, body, bodyLen);
        break;
    default:
        break;  // ADVERTISE / GWINFO from other gateways, encapsulated messages
    }
    processDeferred();
    c = findByAddr(from);
    if (c && c->state == kAwake)
        finishWake(*c, false);
}

void SnGateway::poll(uint32_t now)
{
    now_ = now;
    if (!broker_)
        return;
    processDeferred();
    for (Client &c : clients_) {
        switch (c.state) {
        case kWaitWillTopic:
        case kWaitWillMsg:
        case kWaitConnack:
            if (now - c.lastRx >= 2 * retryMs_)
                lost(c);
            break;
        case kActive:
            if (c.duration && now - c.lastRx >= (uint32_t)c.duration * 1500) {
                lost(c);
                break;
            }
            if ((heldOwner_ == c.index || broker_->hasInflight(c.conn)) && now - c.lastProgress >= retryMs_) {
                if (c.retries >= retryCount_) {
                    lost(c);
                    break;
                }
                ++c.retries;
                c.lastProgress = now;
                if (heldOwner_ == c.index) {
                    sn::Register r;
                    r.topicId = regs_[heldReg_].topicId;
                    r.msgId = heldRegMsgId_;
                    r.topicName = regName(regs_[heldReg_]);
                    sendTo(c, sn::encodeRegister(tx_, sizeof(tx_), r));
                } else {
                    broker_->resendConn(c.conn);
                }
            }
            break;
        case kAsleep:
            if (now - c.lastRx >= (uint32_t)c.duration * 1500)
                lost(c);
            break;
        case kAwake:
            if (heldOwner_ == c.index && now - c.lastProgress >= retryMs_) {
                // resend the REGISTER, as for an active client
                if (c.retries >= retryCount_) {
                    finishWake(c, true);
                    break;
                }
                ++c.retries;
                c.lastProgress = now;
                sn::Register r;
                r.topicId = regs_[heldReg_].topicId;
                r.msgId = heldRegMsgId_;
                r.topicName = regName(regs_[heldReg_]);
                sendTo(c, sn::encodeRegister(tx_, sizeof(tx_), r));
            } else if (now - c.wokeAt >= 2 * retryMs_) {
                finishWake(c, true);  // what is left waits for the next wake-up
            } else {
                broker_->flushConn(c.conn);
                finishWake(c, false);
            }
            break;
        default:
            break;
        }
    }
    if (advertiseSeconds_ && now - lastAdvertise_ >= (uint32_t)advertiseSeconds_ * 1000) {
        lastAdvertise_ = now;
        send(advertiseTo_, sn::encodeAdvertise(tx_, sizeof(tx_), gwId_, advertiseSeconds_));
    }
    processDeferred();
}

// ---- datagrams

void SnGateway::send(const Endpoint &to, size_t n)
{
    if (n && out_)
        out_->sendTo(to, tx_, n);
}

// ---- clients

SnGateway::Client *SnGateway::findByAddr(const Endpoint &a)
{
    for (Client &c : clients_)
        if (c.state != kFree && c.addr == a)
            return &c;
    return nullptr;
}

SnGateway::Client *SnGateway::findById(Bytes id)
{
    for (Client &c : clients_)
        if (c.state != kFree && Bytes(reinterpret_cast<const uint8_t *>(c.clientId), c.clientIdLen).equals(id))
            return &c;
    return nullptr;
}

SnGateway::Client *SnGateway::allocClient()
{
    for (Client &c : clients_)
        if (c.state == kFree)
            return &c;
    return nullptr;
}

void SnGateway::freeClient(Client &c)
{
    for (Registration &r : regs_)
        if (r.client == c.index) {
            pool_.free(r.name);
            r.client = 0xFF;
        }
    pool_.free(c.willTopic);
    c.willTopic = Pool::kNull;
    if (heldOwner_ == c.index) {
        heldOwner_ = -1;
        heldReg_ = -1;
    }
    if (asmOwner_ == c.index) {
        asmOwner_ = -1;
        asmLen_ = 0;
    }
    for (Deferred &d : deferred_)
        if (d.client == c.index)
            d.client = 0xFF;
    c.state = kFree;
    c.conn = -1;
}

void SnGateway::lost(Client &c)
{
    if (c.conn >= 0 && broker_->owns(c.conn, c))
        broker_->linkLost(c.conn);  // publishes the will; close() frees the client
    else
        freeClient(c);
}

void SnGateway::feedDisconnect(Client &c)
{
    if (c.conn < 0 || !broker_->owns(c.conn, c)) {
        freeClient(c);
        return;
    }
    feed(c, [](uint8_t *p, size_t room) {
        Disconnect d;
        return encodeDisconnect(p, room, kMqtt311, PacketType::Disconnect, d);
    });
}

template <typename F>
void SnGateway::feed(Client &c, F encode)
{
    if (c.conn < 0 || !broker_->owns(c.conn, c))
        return;
    size_t room;
    uint8_t *p = broker_->rxBuffer(c.conn, room);
    size_t n = encode(p, room);
    if (n)
        broker_->rxCommit(c.conn, n, now_);
}

void SnGateway::startSession(Client &c, Bytes willPayload)
{
    int conn = broker_->accept(c, now_);
    if (conn < 0) {
        sendTo(c, sn::encodeConnack(tx_, sizeof(tx_), sn::kCongestion));
        freeClient(c);
        return;
    }
    c.conn = conn;
    c.state = kWaitConnack;
    c.connectedAt = now_;
    feed(c, [&](uint8_t *p, size_t room) {
        Connect m;
        m.version = kMqtt311;
        m.cleanStart = c.clean;
        m.keepAlive = 0;  // the gateway watches the MQTT-SN keep alive itself
        m.clientId = Bytes(reinterpret_cast<const uint8_t *>(c.clientId), c.clientIdLen);
        if (c.willTopic != Pool::kNull) {
            m.hasWill = true;
            m.willTopic = Bytes(pool_.ptr(c.willTopic), c.willTopicLen);
            m.willPayload = willPayload;
            m.willQos = c.willQos;
            m.willRetain = c.willRetain;
        }
        size_t n = encodeConnect(p, room, m);
        pool_.free(c.willTopic);  // copied into the packet
        c.willTopic = Pool::kNull;
        return n;
    });
}

// ---- topics

int SnGateway::findRegistration(uint8_t client, Bytes name) const
{
    for (int i = 0; i < TINYMQ_SN_MAX_TOPICS; ++i)
        if (regs_[i].client == client && regName(regs_[i]).equals(name))
            return i;
    return -1;
}

int SnGateway::findRegistration(uint8_t client, uint16_t topicId) const
{
    for (int i = 0; i < TINYMQ_SN_MAX_TOPICS; ++i)
        if (regs_[i].client == client && regs_[i].topicId == topicId)
            return i;
    return -1;
}

int SnGateway::registerTopic(Client &c, Bytes name)
{
    int r = findRegistration(c.index, name);
    if (r >= 0)
        return r;
    if (!validTopicName(name) || name.len > TINYMQ_MAX_TOPIC_LEN)
        return -1;
    for (int i = 0; i < TINYMQ_SN_MAX_TOPICS; ++i) {
        if (regs_[i].client != 0xFF)
            continue;
        uint32_t h = pool_.alloc(name.len);
        if (h == Pool::kNull)
            return -1;
        memcpy(pool_.ptr(h), name.data, name.len);
        if (c.nextTopicId == 0 || c.nextTopicId == 0xFFFF)
            c.nextTopicId = 1;
        regs_[i].client = c.index;
        regs_[i].topicId = c.nextTopicId++;
        regs_[i].name = h;
        regs_[i].nameLen = (uint16_t)name.len;
        return i;
    }
    return -1;
}

bool SnGateway::topicName(Client &c, uint8_t type, uint16_t id, Bytes &name, char shortBuf[2]) const
{
    if (type == sn::kTopicPredefined) {
        const Predefined *p = predefinedById(id);
        if (!p)
            return false;
        name = Bytes(p->name);
        return true;
    }
    if (type == sn::kTopicShort) {
        shortBuf[0] = (char)(id >> 8);
        shortBuf[1] = (char)id;
        name = Bytes(reinterpret_cast<const uint8_t *>(shortBuf), 2);
        return validTopicName(name);
    }
    int r = findRegistration(c.index, id);
    if (r < 0)
        return false;
    name = regName(regs_[r]);
    return true;
}

const SnGateway::Predefined *SnGateway::predefinedByName(Bytes name) const
{
    for (size_t i = 0; i < predefinedCount_; ++i)
        if (name.equals(predefined_[i].name))
            return &predefined_[i];
    return nullptr;
}

const SnGateway::Predefined *SnGateway::predefinedById(uint16_t id) const
{
    for (size_t i = 0; i < predefinedCount_; ++i)
        if (predefined_[i].id == id)
            return &predefined_[i];
    return nullptr;
}

void SnGateway::rememberAck(Client &c, uint16_t msgId, uint16_t topicId)
{
    c.ackMsgId[c.ackNext] = msgId;
    c.ackTopicId[c.ackNext] = topicId;
    c.ackNext = (uint8_t)((c.ackNext + 1) % 8);
}

uint16_t SnGateway::recallAck(Client &c, uint16_t msgId) const
{
    for (int k = 1; k <= 8; ++k) {
        int i = (c.ackNext + 8 - k) % 8;
        if (c.ackMsgId[i] == msgId)
            return c.ackTopicId[i];
    }
    return 0;
}

// ---- MQTT-SN messages from a client

void SnGateway::onConnect(const Endpoint &from, const uint8_t *body, size_t len, uint32_t now)
{
    sn::Connect cp;
    if (!sn::decodeConnect(body, len, cp))
        return;
    if (cp.protocolId != sn::kProtocolId) {
        send(from, sn::encodeConnack(tx_, sizeof(tx_), sn::kNotSupported));
        return;
    }
    Client *c = findByAddr(from);
    if (c) {
        bool sameId = Bytes(reinterpret_cast<const uint8_t *>(c->clientId), c->clientIdLen).equals(cp.clientId);
        if (sameId && (c->state == kAsleep || c->state == kAwake) && !cp.flags.clean) {
            // back from sleep: the session goes on
            c->state = kActive;
            c->duration = cp.duration;
            c->retries = 0;
            c->lastProgress = now;
            sendTo(*c, sn::encodeConnack(tx_, sizeof(tx_), sn::kAccepted));
            broker_->resendConn(c->conn);
            broker_->flushConn(c->conn);
            return;
        }
        if (sameId && c->state == kActive && !cp.flags.will && now - c->connectedAt < 2 * retryMs_) {
            // our CONNACK was lost: answer the resent CONNECT again
            sendTo(*c, sn::encodeConnack(tx_, sizeof(tx_), sn::kAccepted));
            return;
        }
        if (c->conn >= 0)
            feedDisconnect(*c);  // the client is alive: no will
        else
            freeClient(*c);
    }
    c = allocClient();
    if (!c) {
        send(from, sn::encodeConnack(tx_, sizeof(tx_), sn::kCongestion));
        return;
    }
    c->addr = from;
    memcpy(c->clientId, cp.clientId.data, cp.clientId.len);
    c->clientIdLen = (uint8_t)cp.clientId.len;
    c->clean = cp.flags.clean;
    c->duration = cp.duration;
    c->lastRx = now;
    c->lastProgress = now;
    c->retries = 0;
    c->nextTopicId = 1;
    c->conn = -1;
    c->willTopic = Pool::kNull;
    for (uint8_t i = 0; i < 8; ++i)
        c->ackMsgId[i] = 0;
    if (cp.flags.will) {
        c->state = kWaitWillTopic;
        sendTo(*c, sn::encodeEmpty(tx_, sizeof(tx_), sn::kWillTopicReq));
    } else {
        startSession(*c, Bytes());
    }
}

void SnGateway::onPublish(Client *c, const Endpoint &from, const uint8_t *body, size_t len)
{
    sn::Publish p;
    if (!sn::decodePublish(body, len, p))
        return;
    char shortBuf[2];
    Bytes name;
    if (p.flags.qos < 0) {
        // QoS -1: no connection, predefined topic ids and short names only
        if (!qosMinusOne_ || p.flags.topicType == sn::kTopicNormal)
            return;
        if (!topicName(clients_[0], p.flags.topicType, p.topicId, name, shortBuf))
            return;
        broker_->publish(name, p.data, 0, p.flags.retain);
        return;
    }
    if (!c) {
        send(from, sn::encodeDisconnect(tx_, sizeof(tx_), false, 0));  // not connected
        return;
    }
    if (!topicName(*c, p.flags.topicType, p.topicId, name, shortBuf)) {
        sn::TopicAck a;
        a.topicId = p.topicId;
        a.msgId = p.msgId;
        a.rc = sn::kInvalidTopicId;
        sendTo(*c, sn::encodeTopicAck(tx_, sizeof(tx_), sn::kPuback, a));
        return;
    }
    if (p.flags.qos > 0) {
        if (p.msgId == 0)
            return;
        rememberAck(*c, p.msgId, p.topicId);
    }
    feed(*c, [&](uint8_t *buf, size_t room) {
        Publish m;
        m.topic = name;
        m.payload = p.data;
        m.qos = (uint8_t)p.flags.qos;
        m.retain = p.flags.retain;
        m.dup = p.flags.dup;
        m.packetId = p.flags.qos > 0 ? p.msgId : 0;
        return encodePublish(buf, room, kMqtt311, m);
    });
}

void SnGateway::onRegister(Client &c, const uint8_t *body, size_t len)
{
    sn::Register r;
    if (!sn::decodeRegister(body, len, r))
        return;
    sn::TopicAck a;
    a.msgId = r.msgId;
    int i = validTopicName(r.topicName) ? registerTopic(c, r.topicName) : -1;
    if (i >= 0) {
        a.topicId = regs_[i].topicId;
        a.rc = sn::kAccepted;
    } else {
        a.rc = validTopicName(r.topicName) ? sn::kCongestion : sn::kNotSupported;
    }
    sendTo(c, sn::encodeTopicAck(tx_, sizeof(tx_), sn::kRegack, a));
}

void SnGateway::onRegack(Client &c, const uint8_t *body, size_t len)
{
    sn::TopicAck a;
    if (!sn::decodeTopicAck(body, len, a) || heldOwner_ != c.index || a.msgId != heldRegMsgId_)
        return;
    c.retries = 0;
    c.lastProgress = now_;
    if (a.rc != sn::kAccepted && heldReg_ >= 0) {
        pool_.free(regs_[heldReg_].name);
        regs_[heldReg_].client = 0xFF;
        heldReg_ = -1;
    }
    releaseHeld(c, a.rc == sn::kAccepted);
    broker_->flushConn(c.conn);  // what waited behind the held message
}

void SnGateway::onSubscribe(Client &c, uint8_t type, const uint8_t *body, size_t len)
{
    sn::Subscribe s;
    if (!sn::decodeSubscribe(body, len, s) || s.msgId == 0)
        return;
    char shortBuf[2];
    Bytes filter;
    uint16_t replyTopicId = 0;
    if (s.flags.topicType == sn::kTopicNormal) {
        filter = s.topicName;
        if (type == sn::kSubscribe && validTopicName(filter)) {
            int r = registerTopic(c, filter);
            if (r >= 0)
                replyTopicId = regs_[r].topicId;
        }
    } else if (!topicName(c, s.flags.topicType, s.topicId, filter, shortBuf)) {
        if (type == sn::kSubscribe) {
            sn::Suback a;
            a.topicId = s.topicId;
            a.msgId = s.msgId;
            a.rc = sn::kInvalidTopicId;
            sendTo(c, sn::encodeSuback(tx_, sizeof(tx_), a));
        } else {
            sendTo(c, sn::encodeMsgId(tx_, sizeof(tx_), sn::kUnsuback, s.msgId));
        }
        return;
    } else if (s.flags.topicType == sn::kTopicPredefined) {
        replyTopicId = s.topicId;
    }
    if (filter.len > TINYMQ_MAX_TOPIC_LEN)
        return;
    rememberAck(c, s.msgId, replyTopicId);
    uint8_t entries[TINYMQ_MAX_TOPIC_LEN + 3];
    if (type == sn::kSubscribe) {
        SubOptions o;
        o.qos = s.flags.qos < 0 ? 0 : (uint8_t)s.flags.qos;
        size_t n = appendSubscription(entries, sizeof(entries), 0, filter, o);
        feed(c, [&](uint8_t *buf, size_t room) {
            Subscribe m;
            m.packetId = s.msgId;
            m.entries = Bytes(entries, (uint32_t)n);
            return encodeSubscribe(buf, room, kMqtt311, m);
        });
    } else {
        size_t n = appendUnsubscription(entries, sizeof(entries), 0, filter);
        feed(c, [&](uint8_t *buf, size_t room) {
            Unsubscribe m;
            m.packetId = s.msgId;
            m.entries = Bytes(entries, (uint32_t)n);
            return encodeUnsubscribe(buf, room, kMqtt311, m);
        });
    }
}

void SnGateway::onPingreq(Client *c, const Endpoint &from, const uint8_t *body, size_t len, uint32_t now)
{
    if (len == 0) {
        if (c)
            sendTo(*c, sn::encodeEmpty(tx_, sizeof(tx_), sn::kPingresp));
        else
            send(from, sn::encodeDisconnect(tx_, sizeof(tx_), false, 0));
        return;
    }
    // a sleeping client checking for messages: deliver what waits, then PINGRESP
    Client *s = findById(Bytes(body, (uint32_t)len));
    if (!s || (s->state != kAsleep && s->state != kAwake && s->state != kActive)) {
        send(from, sn::encodeDisconnect(tx_, sizeof(tx_), false, 0));
        return;
    }
    s->addr = from;
    s->lastRx = now;
    if (s->state == kActive) {
        sendTo(*s, sn::encodeEmpty(tx_, sizeof(tx_), sn::kPingresp));
        return;
    }
    // awake until the queue is empty (finishWake() sends PINGRESP then)
    s->state = kAwake;
    s->wokeAt = now;
    s->lastProgress = now;
    broker_->resendConn(s->conn);
    broker_->flushConn(s->conn);
    finishWake(*s, false);
}

void SnGateway::finishWake(Client &c, bool force)
{
    if (c.state != kAwake)
        return;
    if (!force && (heldOwner_ == c.index || broker_->hasQueued(c.conn)))
        return;
    c.state = kAsleep;
    c.lastRx = now_;  // the sleep timer starts again
    sendTo(c, sn::encodeEmpty(tx_, sizeof(tx_), sn::kPingresp));
}

void SnGateway::onDisconnect(Client &c, const uint8_t *body, size_t len)
{
    Reader r(body, len);
    if (len == 2 && c.conn >= 0 && (c.state == kActive || c.state == kAwake)) {
        c.duration = r.u16();
        c.state = kAsleep;
        sendTo(c, sn::encodeDisconnect(tx_, sizeof(tx_), false, 0));
        return;
    }
    if (c.conn >= 0)
        feedDisconnect(c);  // the reply is sent by close()
    else {
        sendTo(c, sn::encodeDisconnect(tx_, sizeof(tx_), false, 0));
        freeClient(c);
    }
}

void SnGateway::onWillUpdate(Client &c, uint8_t type, const uint8_t *body, size_t len)
{
    Bytes topic, payload;
    uint8_t qos = 0;
    bool retain = false;
    bool has = broker_->will(c.conn, topic, payload, qos, retain);
    uint8_t rc = sn::kAccepted;
    if (type == sn::kWillTopicUpd) {
        sn::WillTopic w;
        if (!sn::decodeWillTopic(body, len, w))
            return;
        if (!w.present)
            broker_->clearWill(c.conn);
        else if (w.flags.qos < 0 || !broker_->setWill(c.conn, w.topic, has ? payload : Bytes(), (uint8_t)w.flags.qos, w.flags.retain))
            rc = sn::kNotSupported;
        uint8_t b[1] = {rc};
        sendTo(c, sn::encodeRaw(tx_, sizeof(tx_), sn::kWillTopicResp, Bytes(b, 1)));
    } else {
        if (!has || !broker_->setWill(c.conn, topic, Bytes(body, (uint32_t)len), qos, retain))
            rc = sn::kNotSupported;
        uint8_t b[1] = {rc};
        sendTo(c, sn::encodeRaw(tx_, sizeof(tx_), sn::kWillMsgResp, Bytes(b, 1)));
    }
}

void SnGateway::feedAck(Client &c, PacketType type, uint16_t msgId)
{
    feed(c, [&](uint8_t *buf, size_t room) {
        Ack a;
        a.packetId = msgId;
        return encodeAck(buf, room, kMqtt311, type, a);
    });
}

void SnGateway::defer(Client &c, PacketType type, uint16_t msgId)
{
    for (Deferred &d : deferred_)
        if (d.client == 0xFF) {
            d.client = c.index;
            d.type = type;
            d.msgId = msgId;
            return;
        }
}

void SnGateway::processDeferred()
{
    for (Deferred &d : deferred_) {
        if (d.client == 0xFF)
            continue;
        Client &c = clients_[d.client];
        d.client = 0xFF;
        if (c.state != kFree)
            feedAck(c, d.type, d.msgId);
    }
}

// ---- MQTT packets from the broker to a client

size_t SnGateway::Client::write(const uint8_t *data, size_t len)
{
    return gw->clientWrite(*this, data, len);
}

void SnGateway::Client::close()
{
    gw->clientClosed(*this);
}

size_t SnGateway::Client::writable()
{
    if (state != kActive && state != kAwake)
        return 0;  // asleep: the broker keeps the messages queued
    if (gw->heldOwner_ >= 0)
        return 0;  // a PUBLISH waits for REGACK
    return (size_t)-1;
}

void SnGateway::clientClosed(Client &c)
{
    // the broker ended the connection: a DISCONNECT tells the client, except
    // when CONNACK already said it was refused
    bool tell = c.state != kWaitConnack;
    c.conn = -1;
    if (tell)
        sendTo(c, sn::encodeDisconnect(tx_, sizeof(tx_), false, 0));
    freeClient(c);
}

size_t SnGateway::clientWrite(Client &c, const uint8_t *data, size_t len)
{
    if (asmOwner_ != c.index) {
        asmOwner_ = c.index;
        asmLen_ = 0;
    }
    if (len > sizeof(asm_) - asmLen_) {
        asmLen_ = 0;
        return 0;  // larger than a datagram can carry
    }
    memcpy(asm_ + asmLen_, data, len);
    asmLen_ += len;
    for (;;) {
        FixedHeader h;
        HeaderResult r = parseFixedHeader(asm_, asmLen_, h);
        if (r == HeaderResult::NeedMore)
            break;
        size_t total = h.size + (size_t)h.remaining;
        if (r == HeaderResult::Malformed) {
            asmLen_ = 0;
            break;
        }
        if (total > asmLen_)
            break;
        translate(c, h, asm_);
        if (asmOwner_ != c.index)
            break;  // the client was freed meanwhile
        memmove(asm_, asm_ + total, asmLen_ - total);
        asmLen_ -= total;
    }
    return len;
}

void SnGateway::translate(Client &c, const FixedHeader &h, const uint8_t *packet)
{
    const uint8_t *body = packet + h.size;
    size_t len = h.remaining;
    switch (h.type) {
    case PacketType::Connack: {
        Connack a;
        if (decodeConnack(body, len, kMqtt311, a) != kSuccess)
            return;
        uint8_t rc = a.reason == kSuccess ? sn::kAccepted : (a.reason == kServerUnavailable || a.reason == kServerBusy) ? sn::kCongestion : sn::kNotSupported;
        if (a.reason == kSuccess) {
            c.state = kActive;
            c.lastProgress = now_;
            c.retries = 0;
        }
        sendTo(c, sn::encodeConnack(tx_, sizeof(tx_), rc));
        return;
    }
    case PacketType::Publish: {
        Publish p;
        if (decodePublish(h, body, len, kMqtt311, p) != kSuccess)
            return;
        const Predefined *pd = predefinedByName(p.topic);
        if (pd) {
            sendPublish(c, p, pd->id, sn::kTopicPredefined);
            return;
        }
        if (p.topic.len == 2) {
            sendPublish(c, p, (uint16_t)((p.topic.data[0] << 8) | p.topic.data[1]), sn::kTopicShort);
            return;
        }
        int r = findRegistration(c.index, p.topic);
        if (r >= 0) {
            sendPublish(c, p, regs_[r].topicId, sn::kTopicNormal);
            return;
        }
        // register the topic first; the PUBLISH waits for REGACK
        if (heldOwner_ >= 0)
            return;  // a resend while another registration runs: sent again later
        r = registerTopic(c, p.topic);
        if (r < 0 || h.size + len > sizeof(held_)) {
            if (p.qos)
                defer(c, p.qos == 1 ? PacketType::Puback : PacketType::Pubrec, p.packetId);
            return;
        }
        memcpy(held_, packet, h.size + len);
        heldLen_ = h.size + len;
        heldOwner_ = c.index;
        heldReg_ = r;
        if (++c.gwMsgId == 0)
            c.gwMsgId = 1;
        heldRegMsgId_ = c.gwMsgId;
        sn::Register reg;
        reg.topicId = regs_[r].topicId;
        reg.msgId = heldRegMsgId_;
        reg.topicName = regName(regs_[r]);
        c.lastProgress = now_;
        sendTo(c, sn::encodeRegister(tx_, sizeof(tx_), reg));
        return;
    }
    case PacketType::Puback: {
        Ack a;
        if (decodeAck(h, body, len, kMqtt311, a) != kSuccess)
            return;
        sn::TopicAck t;
        t.topicId = recallAck(c, a.packetId);
        t.msgId = a.packetId;
        sendTo(c, sn::encodeTopicAck(tx_, sizeof(tx_), sn::kPuback, t));
        return;
    }
    case PacketType::Pubrec:
    case PacketType::Pubrel:
    case PacketType::Pubcomp: {
        Ack a;
        if (decodeAck(h, body, len, kMqtt311, a) != kSuccess)
            return;
        uint8_t type = h.type == PacketType::Pubrec ? sn::kPubrec : h.type == PacketType::Pubrel ? sn::kPubrel : sn::kPubcomp;
        if (h.type == PacketType::Pubrel)
            c.lastProgress = now_;
        sendTo(c, sn::encodeMsgId(tx_, sizeof(tx_), type, a.packetId));
        return;
    }
    case PacketType::Suback: {
        SubAck a;
        if (decodeSubAck(h, body, len, kMqtt311, a) != kSuccess || a.reasons.len == 0)
            return;
        uint8_t code = a.reasons.data[0];
        sn::Suback s;
        s.flags.qos = code < 0x80 ? (int8_t)code : 0;
        s.topicId = code < 0x80 ? recallAck(c, a.packetId) : 0;
        s.msgId = a.packetId;
        s.rc = code < 0x80 ? sn::kAccepted : sn::kNotSupported;
        sendTo(c, sn::encodeSuback(tx_, sizeof(tx_), s));
        return;
    }
    case PacketType::Unsuback: {
        SubAck a;
        if (decodeSubAck(h, body, len, kMqtt311, a) != kSuccess)
            return;
        sendTo(c, sn::encodeMsgId(tx_, sizeof(tx_), sn::kUnsuback, a.packetId));
        return;
    }
    default:
        return;  // PINGRESP is answered by the gateway itself
    }
}

void SnGateway::sendPublish(Client &c, const Publish &p, uint16_t topicId, uint8_t topicType)
{
    sn::Publish s;
    s.flags.dup = p.dup;
    s.flags.qos = (int8_t)p.qos;
    s.flags.retain = p.retain;
    s.flags.topicType = topicType;
    s.topicId = topicId;
    s.msgId = p.qos ? p.packetId : 0;
    s.data = p.payload;
    size_t n = sn::encodePublish(tx_, sizeof(tx_), s);
    if (n == 0) {
        if (p.qos)
            defer(c, p.qos == 1 ? PacketType::Puback : PacketType::Pubrec, p.packetId);  // cannot be carried: dropped
        return;
    }
    if (p.qos)
        c.lastProgress = now_;
    sendTo(c, n);
}

void SnGateway::releaseHeld(Client &c, bool registered)
{
    int r = heldReg_;
    heldOwner_ = -1;
    heldReg_ = -1;
    FixedHeader h;
    Publish p;
    if (parseFixedHeader(held_, heldLen_, h) != HeaderResult::Ok || decodePublish(h, held_ + h.size, h.remaining, kMqtt311, p) != kSuccess)
        return;
    if (registered && r >= 0)
        sendPublish(c, p, regs_[r].topicId, sn::kTopicNormal);
    else if (p.qos)
        defer(c, p.qos == 1 ? PacketType::Puback : PacketType::Pubrec, p.packetId);
}

}  // namespace tinymq
