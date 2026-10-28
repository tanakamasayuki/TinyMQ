// MQTT-SN 1.2 message codec.
//
// Decoding is zero-copy, as in mq_codec.h: views point into the datagram.
// Encoding writes one whole message and returns its length, or 0 if it
// does not fit.
#pragma once

#include "mq_codec.h"

namespace tinymq {
namespace sn {

enum MsgType : uint8_t {
    kAdvertise = 0x00,
    kSearchGw = 0x01,
    kGwInfo = 0x02,
    kConnect = 0x04,
    kConnack = 0x05,
    kWillTopicReq = 0x06,
    kWillTopic = 0x07,
    kWillMsgReq = 0x08,
    kWillMsg = 0x09,
    kRegister = 0x0A,
    kRegack = 0x0B,
    kPublish = 0x0C,
    kPuback = 0x0D,
    kPubcomp = 0x0E,
    kPubrec = 0x0F,
    kPubrel = 0x10,
    kSubscribe = 0x12,
    kSuback = 0x13,
    kUnsubscribe = 0x14,
    kUnsuback = 0x15,
    kPingreq = 0x16,
    kPingresp = 0x17,
    kDisconnect = 0x18,
    kWillTopicUpd = 0x1A,
    kWillTopicResp = 0x1B,
    kWillMsgUpd = 0x1C,
    kWillMsgResp = 0x1D,
    kEncapsulated = 0xFE,
};

enum TopicType : uint8_t {
    kTopicNormal = 0,      // a registered topic id (PUBLISH) or a topic name (SUBSCRIBE)
    kTopicPredefined = 1,
    kTopicShort = 2,       // two characters carried in the topic id field
};

enum ReturnCode : uint8_t {
    kAccepted = 0x00,
    kCongestion = 0x01,
    kInvalidTopicId = 0x02,
    kNotSupported = 0x03,
};

static const uint8_t kProtocolId = 0x01;

struct Flags {
    bool dup = false;
    int8_t qos = 0;  // -1, 0, 1, 2
    bool retain = false;
    bool will = false;
    bool clean = false;
    uint8_t topicType = kTopicNormal;

    uint8_t encode() const;
    static Flags decode(uint8_t b);
};

// Splits a datagram into message type and body. False if the length field
// does not match the datagram or it is too short.
bool parseHeader(const uint8_t *data, size_t len, uint8_t &type, const uint8_t *&body, size_t &bodyLen);

// ---- messages with a structure worth a struct

struct Connect {
    Flags flags;
    uint8_t protocolId = kProtocolId;
    uint16_t duration = 0;
    Bytes clientId;
};

struct Publish {
    Flags flags;
    uint16_t topicId = 0;
    uint16_t msgId = 0;
    Bytes data;
};

struct Register {
    uint16_t topicId = 0;
    uint16_t msgId = 0;
    Bytes topicName;
};

// REGACK, PUBACK: topic id, message id, return code
struct TopicAck {
    uint16_t topicId = 0;
    uint16_t msgId = 0;
    uint8_t rc = kAccepted;
};

// SUBSCRIBE / UNSUBSCRIBE: a topic name (kTopicNormal) or a topic id
struct Subscribe {
    Flags flags;
    uint16_t msgId = 0;
    uint16_t topicId = 0;
    Bytes topicName;
};

struct Suback {
    Flags flags;
    uint16_t topicId = 0;
    uint16_t msgId = 0;
    uint8_t rc = kAccepted;
};

// WILLTOPIC / WILLTOPICUPD: empty (no flags, no topic) clears the will
struct WillTopic {
    bool present = false;
    Flags flags;
    Bytes topic;
};

bool decodeConnect(const uint8_t *body, size_t len, Connect &out);
bool decodePublish(const uint8_t *body, size_t len, Publish &out);
bool decodeRegister(const uint8_t *body, size_t len, Register &out);
bool decodeTopicAck(const uint8_t *body, size_t len, TopicAck &out);
bool decodeSubscribe(const uint8_t *body, size_t len, Subscribe &out);
bool decodeSuback(const uint8_t *body, size_t len, Suback &out);
bool decodeWillTopic(const uint8_t *body, size_t len, WillTopic &out);
// PUBREC / PUBREL / PUBCOMP / UNSUBACK
bool decodeMsgId(const uint8_t *body, size_t len, uint16_t &msgId);

size_t encodeAdvertise(uint8_t *buf, size_t cap, uint8_t gwId, uint16_t duration);
size_t encodeSearchGw(uint8_t *buf, size_t cap, uint8_t radius);
size_t encodeGwInfo(uint8_t *buf, size_t cap, uint8_t gwId);
size_t encodeConnect(uint8_t *buf, size_t cap, const Connect &c);
size_t encodeConnack(uint8_t *buf, size_t cap, uint8_t rc);
size_t encodeWillTopic(uint8_t *buf, size_t cap, uint8_t type, const WillTopic &w);  // kWillTopic / kWillTopicUpd
size_t encodePublish(uint8_t *buf, size_t cap, const Publish &p);
size_t encodeRegister(uint8_t *buf, size_t cap, const Register &r);
size_t encodeTopicAck(uint8_t *buf, size_t cap, uint8_t type, const TopicAck &a);  // kRegack / kPuback
size_t encodeSubscribe(uint8_t *buf, size_t cap, uint8_t type, const Subscribe &s);  // kSubscribe / kUnsubscribe
size_t encodeSuback(uint8_t *buf, size_t cap, const Suback &s);
size_t encodeMsgId(uint8_t *buf, size_t cap, uint8_t type, uint16_t msgId);  // PUBREC / PUBREL / PUBCOMP / UNSUBACK
size_t encodeDisconnect(uint8_t *buf, size_t cap, bool withDuration, uint16_t duration);
size_t encodePingreq(uint8_t *buf, size_t cap, Bytes clientId);  // clientId empty: none
// WILLTOPICREQ, WILLMSGREQ, PINGRESP and other messages with no body
size_t encodeEmpty(uint8_t *buf, size_t cap, uint8_t type);
// WILLMSG / WILLMSGUPD: the raw message; WILLTOPICRESP / WILLMSGRESP: one return code
size_t encodeRaw(uint8_t *buf, size_t cap, uint8_t type, Bytes body);

}  // namespace sn
}  // namespace tinymq
