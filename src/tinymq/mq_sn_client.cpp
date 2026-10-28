#include "mq_sn_client.h"

#include "mq_topic.h"

namespace tinymq {

bool SnClientEngine::sendTx(size_t n)
{
    if (n == 0)
        return false;
#if TINYMQ_SN_CLIENT_LEVEL >= 1
    lastTx_ = t_.now();
#endif
    return t_.send(tx_, n);
}

// ---- level 0

bool SnClientEngine::publishPredefined(uint16_t topicId, const uint8_t *data, size_t len, int8_t qos, bool retain)
{
#if TINYMQ_SN_CLIENT_LEVEL >= 1
    if (qos >= 0)
        return publishWith(topicId, sn::kTopicPredefined, data, len, (uint8_t)qos, retain);
#endif
    if (qos != -1)
        return false;
    sn::Publish p;
    p.flags.qos = -1;
    p.flags.retain = retain;
    p.flags.topicType = sn::kTopicPredefined;
    p.topicId = topicId;
    p.data = Bytes(data, (uint32_t)len);
    return sendTx(sn::encodePublish(tx_, sizeof(tx_), p));
}

bool SnClientEngine::publishShort(const char *topic, const uint8_t *data, size_t len, int8_t qos, bool retain)
{
    if (!topic || strlen(topic) != 2)
        return false;
    uint16_t id = (uint16_t)(((uint8_t)topic[0] << 8) | (uint8_t)topic[1]);
#if TINYMQ_SN_CLIENT_LEVEL >= 1
    if (qos >= 0)
        return publishWith(id, sn::kTopicShort, data, len, (uint8_t)qos, retain);
#endif
    if (qos != -1)
        return false;
    sn::Publish p;
    p.flags.qos = -1;
    p.flags.retain = retain;
    p.flags.topicType = sn::kTopicShort;
    p.topicId = id;
    p.data = Bytes(data, (uint32_t)len);
    return sendTx(sn::encodePublish(tx_, sizeof(tx_), p));
}

#if TINYMQ_SN_CLIENT_LEVEL >= 1

void SnClientEngine::setRetry(uint32_t ms, uint8_t count)
{
    retryMs_ = ms;
    retryCount_ = count;
}

uint16_t SnClientEngine::nextMsgId()
{
    if (++msgId_ == 0)
        msgId_ = 1;
    return msgId_;
}

bool SnClientEngine::pump()
{
    size_t n = t_.receive(rx_, sizeof(rx_));
    if (n == 0)
        return false;
    uint8_t type;
    const uint8_t *body;
    size_t len;
    if (sn::parseHeader(rx_, n, type, body, len))
        handle(type, body, len);
    return true;
}

bool SnClientEngine::request(size_t n, uint8_t want, uint16_t msgId, bool setDupOnResend)
{
    if (n == 0 || inRequest_)
        return false;
    inRequest_ = true;
    size_t flagsAt = req_[0] == 0x01 ? 4 : 2;
    for (uint8_t attempt = 0; attempt <= retryCount_; ++attempt) {
        if (attempt && setDupOnResend)
            req_[flagsAt] |= 0x80;
        lastTx_ = t_.now();
        t_.send(req_, n);
        uint32_t start = t_.now();
        while (t_.now() - start < retryMs_) {
            size_t got = t_.receive(rx_, sizeof(rx_));
            if (got == 0) {
                t_.idle();
                continue;
            }
            uint8_t type;
            const uint8_t *body;
            size_t len;
            if (!sn::parseHeader(rx_, got, type, body, len))
                continue;
            bool match = type == want;
            if (match && msgId) {
                // the message id sits after the topic id in REGACK / PUBACK / SUBACK,
                // first in PUBREC / PUBCOMP / UNSUBACK
                size_t at = (type == sn::kRegack || type == sn::kPuback) ? 2 : type == sn::kSuback ? 3 : 0;
                match = len >= at + 2 && (uint16_t)((body[at] << 8) | body[at + 1]) == msgId;
            }
            if (match) {
                replyBody_ = body;
                replyLen_ = len;
                inRequest_ = false;
                return true;
            }
            handle(type, body, len);
            if (state_ == kDisconnected && want != sn::kConnack && want != sn::kWillTopicReq && want != sn::kWillMsgReq && want != sn::kGwInfo) {
                inRequest_ = false;
                return false;  // the gateway dropped us meanwhile
            }
        }
    }
    inRequest_ = false;
    return false;
}

void SnClientEngine::lost()
{
    state_ = kDisconnected;
    pingOutstanding_ = false;
}

bool SnClientEngine::connect()
{
    if (inRequest_)
        return false;
    size_t idLen = strlen(clientId_);
    if (idLen == 0 || idLen > 23)
        return false;
    // The topic table is kept: a gateway resuming the session keeps its
    // registrations, and a stale entry is overwritten by the gateway's
    // REGISTER or answered with PUBACK "invalid topic id" (then registered again).
    state_ = kDisconnected;
    sn::Connect c;
    c.flags.clean = clean_;
    c.duration = keepAlive_;
    c.clientId = Bytes(clientId_);
#if TINYMQ_SN_CLIENT_LEVEL >= 2
    c.flags.will = willTopic_ != nullptr;
    if (c.flags.will) {
        if (!request(sn::encodeConnect(req_, sizeof(req_), c), sn::kWillTopicReq, 0))
            return false;
        sn::WillTopic w;
        w.present = true;
        w.flags.qos = (int8_t)willQos_;
        w.flags.retain = willRetain_;
        w.topic = Bytes(willTopic_);
        if (!request(sn::encodeWillTopic(req_, sizeof(req_), sn::kWillTopic, w), sn::kWillMsgReq, 0))
            return false;
        if (!request(sn::encodeRaw(req_, sizeof(req_), sn::kWillMsg, Bytes(willPayload_, (uint32_t)willLen_)), sn::kConnack, 0))
            return false;
    } else
#endif
        if (!request(sn::encodeConnect(req_, sizeof(req_), c), sn::kConnack, 0))
        return false;
    lastRc_ = replyLen_ >= 1 ? replyBody_[0] : 0xFF;
    if (lastRc_ != sn::kAccepted)
        return false;
    state_ = kActive;
    pingOutstanding_ = false;
    return true;
}

void SnClientEngine::disconnect()
{
    if (state_ == kDisconnected)
        return;
    request(sn::encodeDisconnect(req_, sizeof(req_), false, 0), sn::kDisconnect, 0);
    state_ = kDisconnected;
}

int SnClientEngine::findTopic(const char *name, size_t len) const
{
    for (int i = 0; i < TINYMQ_SN_CLIENT_MAX_TOPICS; ++i)
        if (topics_[i].id && strlen(topics_[i].name) == len && memcmp(topics_[i].name, name, len) == 0)
            return i;
    return -1;
}

int SnClientEngine::findTopic(uint16_t id) const
{
    for (int i = 0; i < TINYMQ_SN_CLIENT_MAX_TOPICS; ++i)
        if (topics_[i].id == id)
            return i;
    return -1;
}

void SnClientEngine::remember(uint16_t id, const char *name, size_t len)
{
    if (id == 0 || len > TINYMQ_SN_CLIENT_TOPIC_LEN)
        return;
    int i = findTopic(id);
    if (i < 0)
        i = findTopic(name, len);
    if (i < 0) {
        // a free entry, or the oldest one
        for (int k = 0; k < TINYMQ_SN_CLIENT_MAX_TOPICS && i < 0; ++k)
            if (topics_[k].id == 0)
                i = k;
        if (i < 0) {
            i = topicNext_;
            topicNext_ = (uint8_t)((topicNext_ + 1) % TINYMQ_SN_CLIENT_MAX_TOPICS);
        }
    }
    topics_[i].id = id;
    memcpy(topics_[i].name, name, len);
    topics_[i].name[len] = 0;
}

int SnClientEngine::registerTopic(const char *name)
{
    size_t len = strlen(name);
    int i = findTopic(name, len);
    if (i >= 0)
        return topics_[i].id;
    if (state_ != kActive || !validTopicName(Bytes(name)))
        return -1;
    sn::Register r;
    r.msgId = nextMsgId();
    r.topicName = Bytes(name);
    if (!request(sn::encodeRegister(req_, sizeof(req_), r), sn::kRegack, r.msgId))
        return -1;
    sn::TopicAck a;
    if (!sn::decodeTopicAck(replyBody_, replyLen_, a))
        return -1;
    lastRc_ = a.rc;
    if (a.rc != sn::kAccepted)
        return -1;
    remember(a.topicId, name, len);
    return a.topicId;
}

bool SnClientEngine::publish(const char *topic, const uint8_t *data, size_t len, uint8_t qos, bool retain)
{
    if (strlen(topic) == 2)
        return publishShort(topic, data, len, (int8_t)qos, retain);
    int id = registerTopic(topic);
    if (id < 0)
        return false;
    if (publishWith((uint16_t)id, sn::kTopicNormal, data, len, qos, retain))
        return true;
    if (lastRc_ != sn::kInvalidTopicId)
        return false;
    // the gateway forgot the registration: once more
    int i = findTopic(topic, strlen(topic));
    if (i >= 0)
        topics_[i].id = 0;
    id = registerTopic(topic);
    return id >= 0 && publishWith((uint16_t)id, sn::kTopicNormal, data, len, qos, retain);
}

bool SnClientEngine::publishId(uint16_t topicId, const uint8_t *data, size_t len, uint8_t qos, bool retain)
{
    return publishWith(topicId, sn::kTopicNormal, data, len, qos, retain);
}

bool SnClientEngine::publishWith(uint16_t topicId, uint8_t topicType, const uint8_t *data, size_t len, uint8_t qos, bool retain)
{
#if TINYMQ_SN_CLIENT_LEVEL >= 2
    const uint8_t maxQos = 2;
#else
    const uint8_t maxQos = 1;
#endif
    if (state_ != kActive || qos > maxQos)
        return false;
    sn::Publish p;
    p.flags.qos = (int8_t)qos;
    p.flags.retain = retain;
    p.flags.topicType = topicType;
    p.topicId = topicId;
    p.data = Bytes(data, (uint32_t)len);
    if (qos == 0)
        return sendTx(sn::encodePublish(tx_, sizeof(tx_), p));
    p.msgId = nextMsgId();
    if (qos == 1) {
        if (!request(sn::encodePublish(req_, sizeof(req_), p), sn::kPuback, p.msgId, true))
            return false;
        sn::TopicAck a;
        lastRc_ = sn::decodeTopicAck(replyBody_, replyLen_, a) ? a.rc : 0xFF;
        return lastRc_ == sn::kAccepted;
    }
#if TINYMQ_SN_CLIENT_LEVEL >= 2
    // QoS 2: PUBLISH until PUBREC (or a PUBACK refusing it), then PUBREL until PUBCOMP
    if (!request(sn::encodePublish(req_, sizeof(req_), p), sn::kPubrec, p.msgId, true))
        return false;
    if (!request(sn::encodeMsgId(req_, sizeof(req_), sn::kPubrel, p.msgId), sn::kPubcomp, p.msgId))
        return false;
    lastRc_ = sn::kAccepted;
    return true;
#else
    return false;
#endif
}

int SnClientEngine::subscribe(const char *filter, uint8_t qos)
{
    if (state_ != kActive || !validTopicFilter(Bytes(filter)))
        return -1;
    sn::Subscribe s;
    s.flags.qos = (int8_t)qos;
    s.msgId = nextMsgId();
    size_t len = strlen(filter);
    if (len == 2 && validTopicName(Bytes(filter))) {
        s.flags.topicType = sn::kTopicShort;
        s.topicId = (uint16_t)(((uint8_t)filter[0] << 8) | (uint8_t)filter[1]);
    } else {
        s.topicName = Bytes(filter);
    }
    if (!request(sn::encodeSubscribe(req_, sizeof(req_), sn::kSubscribe, s), sn::kSuback, s.msgId))
        return -1;
    sn::Suback a;
    if (!sn::decodeSuback(replyBody_, replyLen_, a))
        return -1;
    lastRc_ = a.rc;
    if (a.rc != sn::kAccepted)
        return -1;
    if (a.topicId && s.flags.topicType == sn::kTopicNormal)
        remember(a.topicId, filter, len);
    return a.flags.qos;
}

int SnClientEngine::subscribePredefined(uint16_t topicId, uint8_t qos)
{
    if (state_ != kActive)
        return -1;
    sn::Subscribe s;
    s.flags.qos = (int8_t)qos;
    s.flags.topicType = sn::kTopicPredefined;
    s.msgId = nextMsgId();
    s.topicId = topicId;
    if (!request(sn::encodeSubscribe(req_, sizeof(req_), sn::kSubscribe, s), sn::kSuback, s.msgId))
        return -1;
    sn::Suback a;
    if (!sn::decodeSuback(replyBody_, replyLen_, a))
        return -1;
    lastRc_ = a.rc;
    return a.rc == sn::kAccepted ? a.flags.qos : -1;
}

bool SnClientEngine::unsubscribe(const char *filter)
{
    if (state_ != kActive)
        return false;
    sn::Subscribe s;
    s.msgId = nextMsgId();
    size_t len = strlen(filter);
    if (len == 2 && validTopicName(Bytes(filter))) {
        s.flags.topicType = sn::kTopicShort;
        s.topicId = (uint16_t)(((uint8_t)filter[0] << 8) | (uint8_t)filter[1]);
    } else {
        s.topicName = Bytes(filter);
    }
    return request(sn::encodeSubscribe(req_, sizeof(req_), sn::kUnsubscribe, s), sn::kUnsuback, s.msgId);
}

void SnClientEngine::update()
{
    if (inRequest_)
        return;
    for (int i = 0; i < 8 && pump(); ++i) {
    }
    if (state_ != kActive || keepAlive_ == 0)
        return;
    uint32_t now = t_.now();
    if (pingOutstanding_) {
        if (now - pingAt_ < retryMs_)
            return;
        if (pingTries_ >= retryCount_) {
            lost();
            return;
        }
        ++pingTries_;
    } else if (now - lastTx_ < (uint32_t)keepAlive_ * 1000) {
        return;
    } else {
        pingTries_ = 0;
    }
    pingOutstanding_ = true;
    pingAt_ = now;
    sendTx(sn::encodePingreq(tx_, sizeof(tx_), Bytes()));
}

void SnClientEngine::handle(uint8_t type, const uint8_t *body, size_t len)
{
    switch (type) {
    case sn::kPublish: {
        sn::Publish p;
        if (sn::decodePublish(body, len, p))
            deliver(p);
        break;
    }
    case sn::kRegister: {
        sn::Register r;
        if (!sn::decodeRegister(body, len, r))
            break;
        remember(r.topicId, reinterpret_cast<const char *>(r.topicName.data), r.topicName.len);
        sn::TopicAck a;
        a.topicId = r.topicId;
        a.msgId = r.msgId;
        a.rc = r.topicName.len <= TINYMQ_SN_CLIENT_TOPIC_LEN ? sn::kAccepted : sn::kNotSupported;
        sendTx(sn::encodeTopicAck(tx_, sizeof(tx_), sn::kRegack, a));
        break;
    }
#if TINYMQ_SN_CLIENT_LEVEL >= 2
    case sn::kPubrel: {
        uint16_t id;
        if (!sn::decodeMsgId(body, len, id))
            break;
        for (uint16_t &q : qos2In_)
            if (q == id)
                q = 0;
        sendTx(sn::encodeMsgId(tx_, sizeof(tx_), sn::kPubcomp, id));
        break;
    }
#endif
    case sn::kPingresp:
        pingOutstanding_ = false;
        break;
    case sn::kDisconnect:
        if (state_ == kActive)
            lost();  // the gateway dropped us
        break;
    default:
        break;
    }
}

void SnClientEngine::deliver(const sn::Publish &p)
{
    bool fresh = true;
#if TINYMQ_SN_CLIENT_LEVEL >= 2
    if (p.flags.qos == 2) {
        for (uint16_t q : qos2In_)
            if (q == p.msgId)
                fresh = false;
        if (fresh) {
            uint16_t *slot = &qos2In_[0];
            for (uint16_t &q : qos2In_)
                if (q == 0) {
                    slot = &q;
                    break;
                }
            *slot = p.msgId;
        }
    }
#else
    if (p.flags.qos == 2)
        return;  // not at this level
#endif
    char shortName[3];
    const char *name = nullptr;
    bool known = true;
    if (p.flags.topicType == sn::kTopicShort) {
        shortName[0] = (char)(p.topicId >> 8);
        shortName[1] = (char)p.topicId;
        shortName[2] = 0;
        name = shortName;
    } else if (p.flags.topicType == sn::kTopicNormal) {
        int i = findTopic(p.topicId);
        known = i >= 0;
        if (known)
            name = topics_[i].name;
    }
    if (!known) {
        // an id we do not know: refuse it so that the gateway registers again
        sn::TopicAck a;
        a.topicId = p.topicId;
        a.msgId = p.msgId;
        a.rc = sn::kInvalidTopicId;
        sendTx(sn::encodeTopicAck(tx_, sizeof(tx_), sn::kPuback, a));
        return;
    }
    if (fresh && onMessage_) {
        SnMessage m;
        m.topic = name;
        m.topicId = p.topicId;
        m.topicType = p.flags.topicType;
        m.payload = p.data.data;
        m.payloadLen = p.data.len;
        m.qos = p.flags.qos;
        m.retain = p.flags.retain;
        m.dup = p.flags.dup;
        onMessage_(m, onMessageCtx_);
    }
    if (p.flags.qos == 1) {
        sn::TopicAck a;
        a.topicId = p.topicId;
        a.msgId = p.msgId;
        sendTx(sn::encodeTopicAck(tx_, sizeof(tx_), sn::kPuback, a));
    } else if (p.flags.qos == 2) {
        sendTx(sn::encodeMsgId(tx_, sizeof(tx_), sn::kPubrec, p.msgId));
    }
}

#endif  // TINYMQ_SN_CLIENT_LEVEL >= 1

#if TINYMQ_SN_CLIENT_LEVEL >= 2

void SnClientEngine::setWill(const char *topic, const uint8_t *payload, size_t len, uint8_t qos, bool retain)
{
    willTopic_ = topic;
    willPayload_ = payload;
    willLen_ = len;
    willQos_ = qos > 2 ? 2 : qos;
    willRetain_ = retain;
}

bool SnClientEngine::sleep(uint16_t seconds)
{
    if (state_ != kActive)
        return false;
    if (!request(sn::encodeDisconnect(req_, sizeof(req_), true, seconds), sn::kDisconnect, 0))
        return false;
    sleepSeconds_ = seconds;
    state_ = kAsleep;
    return true;
}

bool SnClientEngine::checkMessages()
{
    if (state_ != kAsleep)
        return false;
    // messages arrive (and are answered) before the PINGRESP
    return request(sn::encodePingreq(req_, sizeof(req_), Bytes(clientId_)), sn::kPingresp, 0);
}

int SnClientEngine::searchGateway(uint8_t radius, uint32_t timeoutMs)
{
    size_t n = sn::encodeSearchGw(tx_, sizeof(tx_), radius);
    if (!t_.broadcast(tx_, n))
        return -1;
    uint32_t start = t_.now();
    while (t_.now() - start < timeoutMs) {
        size_t got = t_.receive(rx_, sizeof(rx_));
        if (got == 0) {
            t_.idle();
            continue;
        }
        uint8_t type;
        const uint8_t *body;
        size_t len;
        if (!sn::parseHeader(rx_, got, type, body, len) || len < 1)
            continue;
        if (type == sn::kGwInfo || type == sn::kAdvertise) {
            t_.useLastSender();
            return body[0];
        }
    }
    return -1;
}

#endif  // TINYMQ_SN_CLIENT_LEVEL >= 2

}  // namespace tinymq
