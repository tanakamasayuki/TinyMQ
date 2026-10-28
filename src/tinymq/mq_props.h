// MQTT 5.0 properties: reading, checking and building a property block.
//
// A property block here is the bytes after the property length, as kept in
// the `props` views of mq_codec.h.
#pragma once

#include "mq_codec.h"

namespace tinymq {

enum PropId : uint8_t {
    kPayloadFormatIndicator = 0x01,
    kMessageExpiryInterval = 0x02,
    kContentType = 0x03,
    kResponseTopic = 0x08,
    kCorrelationData = 0x09,
    kSubscriptionIdentifier = 0x0B,
    kSessionExpiryInterval = 0x11,
    kAssignedClientIdentifier = 0x12,
    kServerKeepAlive = 0x13,
    kAuthenticationMethod = 0x15,
    kAuthenticationData = 0x16,
    kRequestProblemInformation = 0x17,
    kWillDelayInterval = 0x18,
    kRequestResponseInformation = 0x19,
    kResponseInformation = 0x1A,
    kServerReference = 0x1C,
    kReasonString = 0x1F,
    kReceiveMaximum = 0x21,
    kTopicAliasMaximum = 0x22,
    kTopicAlias = 0x23,
    kMaximumQos = 0x24,
    kRetainAvailable = 0x25,
    kUserProperty = 0x26,
    kMaximumPacketSize = 0x27,
    kWildcardSubscriptionAvailable = 0x28,
    kSubscriptionIdentifierAvailable = 0x29,
    kSharedSubscriptionAvailable = 0x2A,
};

// Where a property may appear
enum PropScope : uint16_t {
    kInConnect = 1 << 0,
    kInConnack = 1 << 1,
    kInPublish = 1 << 2,
    kInWill = 1 << 3,
    kInAck = 1 << 4,  // PUBACK, PUBREC, PUBREL, PUBCOMP
    kInSubscribe = 1 << 5,
    kInSuback = 1 << 6,
    kInUnsubscribe = 1 << 7,
    kInUnsuback = 1 << 8,
    kInDisconnect = 1 << 9,
    kInAuth = 1 << 10,
};

struct Property {
    uint8_t id;
    uint32_t value;  // byte, two-byte, four-byte and variable byte integers
    Bytes data;      // string, binary data, or a user property's name
    Bytes data2;     // a user property's value
};

class PropReader {
public:
    explicit PropReader(Bytes props) : r_(props.data, props.len), len_(props.len) {}
    // The next property; false at the end or on a malformed one (see ok()).
    bool next(Property &p);
    bool ok() const { return r_.ok(); }
    size_t offset() const { return len_ - r_.remaining(); }  // bytes read so far

private:
    Reader r_;
    size_t len_;
};

// Checks types, value ranges, UTF-8, where each property may appear and
// that none appears twice (except User Property, and Subscription
// Identifier in PUBLISH). Returns kSuccess, kMalformedPacket or kProtocolError.
uint8_t checkProps(Bytes props, uint16_t scope);

// The first property `id`, if present
bool findProp(Bytes props, uint8_t id, Property &out);

// Builds a property block into a caller buffer.
class PropWriter {
public:
    PropWriter(uint8_t *buf, size_t cap) : w_(buf, cap), buf_(buf) {}

    void byte(uint8_t id, uint8_t v);
    void u16(uint8_t id, uint16_t v);
    void u32(uint8_t id, uint32_t v);
    void varint(uint8_t id, uint32_t v);
    void string(uint8_t id, Bytes s);  // also binary data
    void pair(uint8_t id, Bytes name, Bytes value);
    // Appends properties of another block, leaving out the ids in `skip`
    void copy(Bytes props, const uint8_t *skip = nullptr, size_t skipCount = 0);

    bool ok() const { return w_.ok(); }
    Bytes result() const { return Bytes(buf_, (uint32_t)w_.pos()); }

private:
    Writer w_;
    uint8_t *buf_;
};

}  // namespace tinymq
