#include "TinyMQClient.h"

TinyMQClient::TinyMQClient(Client &client) : client_(client) {}

void TinyMQClient::setServer(IPAddress ip, uint16_t port)
{
    ip_ = ip;
    host_ = nullptr;
    port_ = port;
}

void TinyMQClient::setServer(const char *host, uint16_t port)
{
    host_ = host;
    port_ = port;
}

void TinyMQClient::setWill(const char *topic, const char *payload, uint8_t qos, bool retain)
{
    engine_.setWill(topic, reinterpret_cast<const uint8_t *>(payload), payload ? strlen(payload) : 0, qos, retain);
}

void TinyMQClient::setConnectTimeout(uint32_t ms)
{
    connectTimeout_ = ms;
    engine_.setConnackTimeout(ms);
}

void TinyMQClient::setReconnectInterval(uint32_t ms)
{
    reconnectInterval_ = ms;
}

bool TinyMQClient::openAndStart()
{
    lastAttempt_ = millis();
    if (engine_.state() != tinymq::ClientEngine::State::Disconnected)
        engine_.disconnect();
    int ok = host_ ? client_.connect(host_, port_) : client_.connect(ip_, port_);
    if (!ok)
        return false;
    return engine_.start(*this, millis());
}

bool TinyMQClient::connect()
{
    wanted_ = true;
    if (!openAndStart())
        return false;
    uint32_t start = millis();
    while (engine_.state() == tinymq::ClientEngine::State::WaitConnack) {
        update();
        if (millis() - start > connectTimeout_ + 100)
            break;
        delay(1);
    }
    return engine_.connected();
}

bool TinyMQClient::connect(const char *clientId)
{
    engine_.setClientId(clientId);
    return connect();
}

void TinyMQClient::disconnect()
{
    wanted_ = false;
    engine_.disconnect();
}

void TinyMQClient::readAvailable()
{
    // a bounded number of reads so that one busy connection cannot hold loop()
    for (int i = 0; i < 8 && engine_.state() != tinymq::ClientEngine::State::Disconnected; ++i) {
        int avail = client_.available();
        if (avail <= 0)
            break;
        size_t room;
        uint8_t *p = engine_.rxBuffer(room);
        if (room == 0)
            break;
        int n = client_.read(p, (size_t)avail < room ? (size_t)avail : room);
        if (n <= 0)
            break;
        engine_.rxCommit((size_t)n, millis());
    }
}

void TinyMQClient::update()
{
    if (engine_.state() != tinymq::ClientEngine::State::Disconnected) {
        readAvailable();
        if (engine_.state() != tinymq::ClientEngine::State::Disconnected && !client_.connected() && client_.available() <= 0)
            engine_.linkLost();
        engine_.poll(millis());
    } else if (wanted_ && reconnectInterval_ && millis() - lastAttempt_ >= reconnectInterval_) {
        openAndStart();  // CONNACK is handled by later update() calls
    }
}

int TinyMQClient::publish(const char *topic, const uint8_t *payload, size_t len, uint8_t qos, bool retain)
{
    return engine_.publish(topic, payload, len, qos, retain);
}

int TinyMQClient::publish(const char *topic, const char *payload, uint8_t qos, bool retain)
{
    return engine_.publish(topic, reinterpret_cast<const uint8_t *>(payload), payload ? strlen(payload) : 0, qos, retain);
}

size_t TinyMQClient::write(const uint8_t *data, size_t len)
{
    size_t done = 0;
    while (done < len) {
        size_t n = client_.write(data + done, len - done);
        if (n == 0)
            break;
        done += n;
    }
    return done;
}

void TinyMQClient::close()
{
    client_.stop();
}
