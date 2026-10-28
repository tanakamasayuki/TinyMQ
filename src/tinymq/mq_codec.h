// MQTT 3.1 / 3.1.1 / 5.0 packet codec.
//
// Decoding is zero-copy: strings, binary data and payloads in the decoded
// structs point into the packet buffer, which must outlive them. 5.0
// properties are kept as the raw property block (`props`) for now.
//
// Encoding writes a whole packet into a caller buffer and returns its
// length, or 0 if it does not fit. encodePublishHead() writes everything
// but the payload, so a large payload can be sent from where it lies.
#pragma once

#include "mq_types.h"

namespace tinymq {

// ---- primitives

// Reads big-endian fields from a packet body. Any read past the end, or an
// invalid field, clears ok() and returns zero / an empty view from then on.
class Reader {
public:
    Reader(const uint8_t *data, size_t len) : p_(data), len_(len), pos_(0), ok_(true) {}

    uint8_t u8();
    uint16_t u16();
    uint32_t u32();
    uint32_t varint();
    Bytes str();       // UTF-8 string, validated (no U+0000, no surrogates, no overlong forms)
    Bytes bin();       // binary data
    Bytes rest();      // everything left
    Bytes take(size_t n);

    bool ok() const { return ok_; }
    size_t remaining() const { return ok_ ? len_ - pos_ : 0; }
    bool atEnd() const { return ok_ && pos_ == len_; }
    void fail() { ok_ = false; }

private:
    const uint8_t *p_;
    size_t len_;
    size_t pos_;
    bool ok_;
};

// Writes big-endian fields. With a null buffer it only counts.
class Writer {
public:
    Writer(uint8_t *buf, size_t cap) : buf_(buf), cap_(cap), pos_(0), ok_(true) {}

    void u8(uint8_t v);
    void u16(uint16_t v);
    void u32(uint32_t v);
    void varint(uint32_t v);
    void str(Bytes s);   // length-prefixed
    void raw(const uint8_t *d, size_t n);
    void raw(Bytes b) { raw(b.data, b.len); }

    bool ok() const { return ok_; }
    size_t pos() const { return pos_; }

private:
    uint8_t *buf_;
    size_t cap_;
    size_t pos_;
    bool ok_;
};

bool validUtf8(const uint8_t *s, size_t n);
size_t varintSize(uint32_t v);

// ---- fixed header

struct FixedHeader {
    PacketType type;
    uint8_t flags;       // low nibble of the first byte
    uint32_t remaining;  // remaining length
    uint8_t size;        // bytes taken by the fixed header itself
};

enum class HeaderResult : uint8_t { Ok, NeedMore, Malformed };

// Parses the fixed header at the start of `data`. Checks the reserved flag
// bits of each packet type.
HeaderResult parseFixedHeader(const uint8_t *data, size_t len, FixedHeader &out);

// ---- packets

struct Connect {
    uint8_t version = kMqtt311;  // kMqtt31 / kMqtt311 / kMqtt5
    bool cleanStart = true;
    uint16_t keepAlive = 60;
    Bytes clientId;
    bool hasWill = false;
    uint8_t willQos = 0;
    bool willRetain = false;
    Bytes willProps;  // 5.0
    Bytes willTopic;
    Bytes willPayload;
    bool hasUsername = false;
    Bytes username;
    bool hasPassword = false;
    Bytes password;
    Bytes props;  // 5.0
};

struct Connack {
    bool sessionPresent = false;
    uint8_t reason = kSuccess;
    Bytes props;  // 5.0
};

struct Publish {
    uint8_t qos = 0;
    bool retain = false;
    bool dup = false;
    Bytes topic;
    uint16_t packetId = 0;  // QoS 1 / 2 only
    Bytes props;            // 5.0
    Bytes payload;
};

// PUBACK / PUBREC / PUBREL / PUBCOMP
struct Ack {
    uint16_t packetId = 0;
    uint8_t reason = kSuccess;  // 5.0
    Bytes props;                // 5.0
};

// SUBSCRIBE, UNSUBSCRIBE: `entries` holds the topic filters (each followed by
// the options byte for SUBSCRIBE). Walk them with nextSubscription() /
// nextUnsubscription().
struct Subscribe {
    uint16_t packetId = 0;
    Bytes props;  // 5.0
    Bytes entries;
};

// Subscription options byte (5.0 layout; 3.1.1 uses only the QoS bits)
struct SubOptions {
    uint8_t qos = 0;
    bool noLocal = false;
    bool retainAsPublished = false;
    uint8_t retainHandling = 0;

    uint8_t encode() const;
    static SubOptions decode(uint8_t b);
};

struct Unsubscribe {
    uint16_t packetId = 0;
    Bytes props;  // 5.0
    Bytes entries;
};

// SUBACK / UNSUBACK: one reason code per filter (UNSUBACK has none before 5.0)
struct SubAck {
    uint16_t packetId = 0;
    Bytes props;  // 5.0
    Bytes reasons;
};

// DISCONNECT / AUTH
struct Disconnect {
    uint8_t reason = kSuccess;  // 5.0
    Bytes props;                // 5.0
};

// Decoding: `body` is the packet after the fixed header, `remaining` bytes
// long. Returns kSuccess, kMalformedPacket or kProtocolError;
// decodeConnect() may also return kUnsupportedProtocolVersion, and then
// out.version holds the level the client asked for.
uint8_t decodeConnect(const uint8_t *body, size_t len, Connect &out);
uint8_t decodeConnack(const uint8_t *body, size_t len, uint8_t version, Connack &out);
uint8_t decodePublish(const FixedHeader &h, const uint8_t *body, size_t len, uint8_t version, Publish &out);
uint8_t decodeAck(const FixedHeader &h, const uint8_t *body, size_t len, uint8_t version, Ack &out);
uint8_t decodeSubscribe(const uint8_t *body, size_t len, uint8_t version, Subscribe &out);
uint8_t decodeUnsubscribe(const uint8_t *body, size_t len, uint8_t version, Unsubscribe &out);
uint8_t decodeSubAck(const FixedHeader &h, const uint8_t *body, size_t len, uint8_t version, SubAck &out);
uint8_t decodeDisconnect(const FixedHeader &h, const uint8_t *body, size_t len, uint8_t version, Disconnect &out);

// Walks the entries of a decoded SUBSCRIBE / UNSUBSCRIBE. `pos` starts at 0.
bool nextSubscription(const Subscribe &s, size_t &pos, Bytes &filter, SubOptions &opts);
bool nextUnsubscription(const Unsubscribe &u, size_t &pos, Bytes &filter);

// Encoding
size_t encodeConnect(uint8_t *buf, size_t cap, const Connect &c);
size_t encodeConnack(uint8_t *buf, size_t cap, uint8_t version, const Connack &c);
size_t encodePublish(uint8_t *buf, size_t cap, uint8_t version, const Publish &p);
size_t encodePublishHead(uint8_t *buf, size_t cap, uint8_t version, const Publish &p);
size_t encodeAck(uint8_t *buf, size_t cap, uint8_t version, PacketType type, const Ack &a);
// `entries` of a Subscribe / Unsubscribe to encode is built with
// appendSubscription() / appendUnsubscription() into a caller buffer.
size_t encodeSubscribe(uint8_t *buf, size_t cap, uint8_t version, const Subscribe &s);
size_t encodeUnsubscribe(uint8_t *buf, size_t cap, uint8_t version, const Unsubscribe &u);
size_t encodeSubAck(uint8_t *buf, size_t cap, uint8_t version, PacketType type, const SubAck &a);
size_t encodePingreq(uint8_t *buf, size_t cap);
size_t encodePingresp(uint8_t *buf, size_t cap);
size_t encodeDisconnect(uint8_t *buf, size_t cap, uint8_t version, PacketType type, const Disconnect &d);

size_t appendSubscription(uint8_t *buf, size_t cap, size_t pos, Bytes filter, SubOptions opts);
size_t appendUnsubscription(uint8_t *buf, size_t cap, size_t pos, Bytes filter);

// 3.1.1 CONNACK return code <-> 5.0 reason code
uint8_t connackCodeFromReason(uint8_t reason);
uint8_t reasonFromConnackCode(uint8_t code);

}  // namespace tinymq
