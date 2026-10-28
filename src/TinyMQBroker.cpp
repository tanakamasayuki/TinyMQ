#include "TinyMQBroker.h"

bool TinyMQBroker::addListener(TinyMQListener &listener)
{
    if (listenerCount_ >= TINYMQ_BROKER_MAX_LISTENERS)
        return false;
    listeners_[listenerCount_++] = &listener;
    return true;
}

void TinyMQBroker::update()
{
    for (size_t i = 0; i < listenerCount_; ++i)
        listeners_[i]->update(engine_, millis());
    engine_.poll(millis());
}

bool TinyMQBroker::publish(const char *topic, const char *payload, uint8_t qos, bool retain)
{
    return engine_.publish(topic, reinterpret_cast<const uint8_t *>(payload), payload ? strlen(payload) : 0, qos, retain);
}
