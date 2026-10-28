// Types shared by the codecs, the client and the broker.
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string.h>

namespace tinymq {

// A view of bytes owned by someone else (usually the packet buffer).
// Strings are not NUL-terminated.
struct Bytes {
    const uint8_t *data;
    uint32_t len;

    Bytes() : data(nullptr), len(0) {}
    Bytes(const uint8_t *d, uint32_t n) : data(d), len(n) {}
    explicit Bytes(const char *s) : data(reinterpret_cast<const uint8_t *>(s)), len(s ? (uint32_t)strlen(s) : 0) {}

    bool equals(Bytes o) const { return len == o.len && (len == 0 || memcmp(data, o.data, len) == 0); }
    bool equals(const char *s) const { return equals(Bytes(s)); }
};

// Protocol level byte of CONNECT
enum : uint8_t {
    kMqtt31 = 3,   // "MQIsdp"
    kMqtt311 = 4,
    kMqtt5 = 5,
};

enum class PacketType : uint8_t {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Pubrec = 5,
    Pubrel = 6,
    Pubcomp = 7,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
    Auth = 15,
};

// MQTT 5.0 reason codes. Used internally for every version; the 3.1.1
// codec maps them to its return codes.
enum Reason : uint8_t {
    kSuccess = 0x00,
    kGrantedQos1 = 0x01,
    kGrantedQos2 = 0x02,
    kDisconnectWithWill = 0x04,
    kNoMatchingSubscribers = 0x10,
    kNoSubscriptionExisted = 0x11,
    kContinueAuthentication = 0x18,
    kReAuthenticate = 0x19,
    kUnspecifiedError = 0x80,
    kMalformedPacket = 0x81,
    kProtocolError = 0x82,
    kImplementationSpecificError = 0x83,
    kUnsupportedProtocolVersion = 0x84,
    kClientIdentifierNotValid = 0x85,
    kBadUserNameOrPassword = 0x86,
    kNotAuthorized = 0x87,
    kServerUnavailable = 0x88,
    kServerBusy = 0x89,
    kBanned = 0x8A,
    kServerShuttingDown = 0x8B,
    kBadAuthenticationMethod = 0x8C,
    kKeepAliveTimeout = 0x8D,
    kSessionTakenOver = 0x8E,
    kTopicFilterInvalid = 0x8F,
    kTopicNameInvalid = 0x90,
    kPacketIdentifierInUse = 0x91,
    kPacketIdentifierNotFound = 0x92,
    kReceiveMaximumExceeded = 0x93,
    kTopicAliasInvalid = 0x94,
    kPacketTooLarge = 0x95,
    kMessageRateTooHigh = 0x96,
    kQuotaExceeded = 0x97,
    kAdministrativeAction = 0x98,
    kPayloadFormatInvalid = 0x99,
    kRetainNotSupported = 0x9A,
    kQosNotSupported = 0x9B,
    kUseAnotherServer = 0x9C,
    kServerMoved = 0x9D,
    kSharedSubscriptionsNotSupported = 0x9E,
    kConnectionRateExceeded = 0x9F,
    kMaximumConnectTime = 0xA0,
    kSubscriptionIdentifiersNotSupported = 0xA1,
    kWildcardSubscriptionsNotSupported = 0xA2,
};

// The stream under one MQTT connection, provided by the transport.
class Link {
public:
    virtual ~Link() {}
    // Writes all of `data` or fails (returns less).
    virtual size_t write(const uint8_t *data, size_t len) = 0;
    virtual void close() = 0;
    // Bytes that can be written now without blocking
    virtual size_t writable() { return (size_t)-1; }
};

// A received application message. `topic` is NUL-terminated; the views are
// valid only during the callback.
struct Message {
    const char *topic;
    uint16_t topicLen;
    const uint8_t *payload;
    size_t payloadLen;
    uint8_t qos;
    bool retain;
    bool dup;
    Bytes props;  // 5.0
};

}  // namespace tinymq
