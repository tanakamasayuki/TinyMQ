#include "TinyMQBridge.h"

#include "tinymq/mq_topic.h"

TinyMQBridge::TinyMQBridge(Client &upstream) : client_(upstream)
{
#if TINYMQ_BRIDGE_QUEUE_BYTES > 0
    pool_.begin(arena_, sizeof(arena_));
#endif
    client_.onMessage(onRemote, this);
    client_.onConnect(onConnack, this);
}

bool TinyMQBridge::addTopic(const char *pattern, Direction direction, uint8_t qos, const char *localPrefix, const char *remotePrefix)
{
    if (topicCount_ >= TINYMQ_BRIDGE_MAX_TOPICS || started_ || qos > 2 || !(direction & kBoth))
        return false;
    char a[TINYMQ_MAX_TOPIC_LEN + 1], b[TINYMQ_MAX_TOPIC_LEN + 1];
    if (!join(a, localPrefix, pattern) || !join(b, remotePrefix, pattern) || !tinymq::validTopicFilter(tinymq::Bytes(a)) ||
        !tinymq::validTopicFilter(tinymq::Bytes(b)))
        return false;
    Topic &t = topics_[topicCount_++];
    t.pattern = pattern;
    t.localPrefix = localPrefix;
    t.remotePrefix = remotePrefix;
    t.direction = direction;
    t.qos = qos;
    return true;
}

bool TinyMQBridge::join(char *buf, const char *prefix, const char *pattern)
{
    size_t a = strlen(prefix), b = strlen(pattern);
    if (a + b > TINYMQ_MAX_TOPIC_LEN)
        return false;
    memcpy(buf, prefix, a);
    memcpy(buf + a, pattern, b + 1);
    return true;
}

bool TinyMQBridge::rewrite(char *buf, const char *topic, const char *from, const char *to)
{
    size_t f = strlen(from);
    if (strncmp(topic, from, f) != 0)
        return false;
    return join(buf, to, topic + f);
}

void TinyMQBridge::update(tinymq::Broker &engine, uint32_t now)
{
    (void)now;
    if (!started_) {
        started_ = true;
        engine_ = &engine;
        tap_ = engine.addTap(onLocal, this);
        client_.setReconnectInterval(reconnectMs_);
        client_.connectAsync();
    }
    client_.update();
    if (queued_ && client_.connected())
        sendKept();
}

void TinyMQBridge::keep(const tinymq::Message &m, uint8_t qos)
{
#if TINYMQ_BRIDGE_QUEUE_BYTES > 0
    if (qos > 0 && queued_ < TINYMQ_BRIDGE_MAX_QUEUED) {
        uint32_t h = store_.create(tinymq::Bytes(topic_), m.props, tinymq::Bytes(m.payload, (uint32_t)m.payloadLen), qos, m.retain);
        if (h != tinymq::Pool::kNull) {
            queue_[(queueHead_ + queued_++) % TINYMQ_BRIDGE_MAX_QUEUED] = h;
            return;
        }
    }
#else
    (void)m;
    (void)qos;
#endif
    ++dropped_;
}

void TinyMQBridge::sendKept()
{
#if TINYMQ_BRIDGE_QUEUE_BYTES > 0
    // oldest first; what the client cannot take now waits for the next update()
    while (queued_ && client_.connected()) {
        uint32_t h = queue_[queueHead_];
        tinymq::Bytes t = store_.topic(h);
        memcpy(topic_, t.data, t.len);
        topic_[t.len] = 0;
        tinymq::Bytes p = store_.payload(h);
        if (client_.publish(topic_, p.data, p.len, store_.qos(h), store_.retain(h), store_.props(h)) >= 0)
            ++out_;
        else if (client_.inflight() > 0)
            return;      // upstream is busy: try again later
        else
            ++dropped_;  // upstream will never take it (too large, QoS, ...)
        store_.release(h);
        queueHead_ = (queueHead_ + 1) % TINYMQ_BRIDGE_MAX_QUEUED;
        --queued_;
    }
#endif
}

void TinyMQBridge::onConnack(uint8_t reason, bool sessionPresent, void *self)
{
    (void)sessionPresent;
    if (reason == tinymq::kSuccess)
        static_cast<TinyMQBridge *>(self)->subscribeAll();
}

void TinyMQBridge::subscribeAll()
{
    bool v5 = client_.engine().protocolVersion() == tinymq::kMqtt5;
    for (size_t i = 0; i < topicCount_; ++i) {
        const Topic &t = topics_[i];
        if (t.direction & kIn) {
            join(filter_, t.remotePrefix, t.pattern);
            tinymq::SubOptions o;
            o.qos = t.qos;
            o.noLocal = v5;           // our own messages do not come back
            o.retainAsPublished = v5; // keep the retain flag
            client_.subscribe(filter_, o);
        }
    }
    // the local side, (again) now that upstream is there: retained messages
    // go up as they are delivered to the tap
    for (size_t i = 0; i < topicCount_; ++i) {
        const Topic &t = topics_[i];
        if (t.direction & kOut) {
            char f[TINYMQ_MAX_TOPIC_LEN + 1];
            join(f, t.localPrefix, t.pattern);
            engine_->tapSubscribe(tap_, f, t.qos);
        }
    }
}

void TinyMQBridge::onLocal(const tinymq::Message &m, void *self)
{
    TinyMQBridge &b = *static_cast<TinyMQBridge *>(self);
    for (size_t i = 0; i < b.topicCount_; ++i) {
        const Topic &t = b.topics_[i];
        if (!(t.direction & kOut))
            continue;
        join(b.filter_, t.localPrefix, t.pattern);
        if (!tinymq::topicMatches(tinymq::Bytes(b.filter_), tinymq::Bytes(reinterpret_cast<const uint8_t *>(m.topic), m.topicLen)))
            continue;
        if (!rewrite(b.topic_, m.topic, t.localPrefix, t.remotePrefix)) {
            ++b.dropped_;
            return;
        }
        uint8_t qos = m.qos < t.qos ? m.qos : t.qos;
        // in order: behind what is kept, or kept when upstream cannot take it now
        if (b.queued_ || !b.client_.connected() || b.client_.publish(b.topic_, m.payload, m.payloadLen, qos, m.retain, m.props) < 0)
            b.keep(m, qos);
        else
            ++b.out_;
        return;
    }
}

void TinyMQBridge::onRemote(const tinymq::Message &m, void *self)
{
    TinyMQBridge &b = *static_cast<TinyMQBridge *>(self);
    for (size_t i = 0; i < b.topicCount_; ++i) {
        const Topic &t = b.topics_[i];
        if (!(t.direction & kIn))
            continue;
        join(b.filter_, t.remotePrefix, t.pattern);
        if (!tinymq::topicMatches(tinymq::Bytes(b.filter_), tinymq::Bytes(reinterpret_cast<const uint8_t *>(m.topic), m.topicLen)))
            continue;
        if (!rewrite(b.topic_, m.topic, t.remotePrefix, t.localPrefix)) {
            ++b.dropped_;
            return;
        }
        uint8_t qos = m.qos < t.qos ? m.qos : t.qos;
        if (b.engine_ && b.engine_->tapPublish(b.tap_, tinymq::Bytes(b.topic_), tinymq::Bytes(m.payload, (uint32_t)m.payloadLen), qos, m.retain, m.props))
            ++b.in_;
        else
            ++b.dropped_;
        return;
    }
}
