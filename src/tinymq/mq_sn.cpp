#include "mq_sn.h"

namespace tinymq {
namespace sn {

uint8_t Flags::encode() const
{
    uint8_t q = qos < 0 ? 3 : (uint8_t)qos;
    return (uint8_t)((dup ? 0x80 : 0) | ((q & 3) << 5) | (retain ? 0x10 : 0) | (will ? 0x08 : 0) | (clean ? 0x04 : 0) | (topicType & 3));
}

Flags Flags::decode(uint8_t b)
{
    Flags f;
    f.dup = b & 0x80;
    uint8_t q = (b >> 5) & 3;
    f.qos = q == 3 ? -1 : (int8_t)q;
    f.retain = b & 0x10;
    f.will = b & 0x08;
    f.clean = b & 0x04;
    f.topicType = b & 3;
    return f;
}

bool parseHeader(const uint8_t *data, size_t len, uint8_t &type, const uint8_t *&body, size_t &bodyLen)
{
    if (len < 2)
        return false;
    size_t total, head;
    if (data[0] == 0x01) {
        if (len < 4)
            return false;
        total = ((size_t)data[1] << 8) | data[2];
        head = 4;
        if (total < 256)
            return false;  // the long form is only for messages of 256 octets or more
    } else {
        total = data[0];
        head = 2;
        if (total < 2)
            return false;
    }
    if (total != len)
        return false;
    type = data[head - 1];
    body = data + head;
    bodyLen = len - head;
    return true;
}

// ---- decoding

bool decodeConnect(const uint8_t *body, size_t len, Connect &out)
{
    Reader r(body, len);
    out.flags = Flags::decode(r.u8());
    out.protocolId = r.u8();
    out.duration = r.u16();
    out.clientId = r.rest();
    return r.ok() && out.clientId.len >= 1 && out.clientId.len <= 23 && validUtf8(out.clientId.data, out.clientId.len);
}

bool decodePublish(const uint8_t *body, size_t len, Publish &out)
{
    Reader r(body, len);
    out.flags = Flags::decode(r.u8());
    out.topicId = r.u16();
    out.msgId = r.u16();
    out.data = r.rest();
    return r.ok() && out.flags.topicType != 3;
}

bool decodeRegister(const uint8_t *body, size_t len, Register &out)
{
    Reader r(body, len);
    out.topicId = r.u16();
    out.msgId = r.u16();
    out.topicName = r.rest();
    return r.ok() && out.topicName.len > 0 && validUtf8(out.topicName.data, out.topicName.len);
}

bool decodeTopicAck(const uint8_t *body, size_t len, TopicAck &out)
{
    Reader r(body, len);
    out.topicId = r.u16();
    out.msgId = r.u16();
    out.rc = r.u8();
    return r.atEnd();
}

bool decodeSubscribe(const uint8_t *body, size_t len, Subscribe &out)
{
    Reader r(body, len);
    out.flags = Flags::decode(r.u8());
    out.msgId = r.u16();
    out.topicId = 0;
    out.topicName = Bytes();
    if (out.flags.topicType == kTopicNormal) {
        out.topicName = r.rest();
        return r.ok() && out.topicName.len > 0 && validUtf8(out.topicName.data, out.topicName.len);
    }
    out.topicId = r.u16();
    return r.atEnd() && out.flags.topicType != 3;
}

bool decodeSuback(const uint8_t *body, size_t len, Suback &out)
{
    Reader r(body, len);
    out.flags = Flags::decode(r.u8());
    out.topicId = r.u16();
    out.msgId = r.u16();
    out.rc = r.u8();
    return r.atEnd();
}

bool decodeWillTopic(const uint8_t *body, size_t len, WillTopic &out)
{
    out.present = len > 0;
    out.flags = Flags();
    out.topic = Bytes();
    if (!out.present)
        return true;
    Reader r(body, len);
    out.flags = Flags::decode(r.u8());
    out.topic = r.rest();
    return r.ok() && out.topic.len > 0 && validUtf8(out.topic.data, out.topic.len);
}

bool decodeMsgId(const uint8_t *body, size_t len, uint16_t &msgId)
{
    Reader r(body, len);
    msgId = r.u16();
    return r.atEnd();
}

// ---- encoding

// Runs `body` once to count, then writes the length field (short or long
// form), the type and the body.
template <typename F>
static size_t emit(uint8_t *buf, size_t cap, uint8_t type, F body)
{
    Writer counter(nullptr, 0);
    body(counter);
    size_t total = counter.pos() + 2;
    bool longForm = total >= 256;
    if (longForm)
        total += 2;
    if (!counter.ok() || total > 0xFFFF)
        return 0;
    Writer w(buf, cap);
    if (longForm) {
        w.u8(0x01);
        w.u16((uint16_t)total);
    } else {
        w.u8((uint8_t)total);
    }
    w.u8(type);
    body(w);
    return w.ok() ? w.pos() : 0;
}

size_t encodeAdvertise(uint8_t *buf, size_t cap, uint8_t gwId, uint16_t duration)
{
    return emit(buf, cap, kAdvertise, [&](Writer &w) {
        w.u8(gwId);
        w.u16(duration);
    });
}

size_t encodeSearchGw(uint8_t *buf, size_t cap, uint8_t radius)
{
    return emit(buf, cap, kSearchGw, [&](Writer &w) { w.u8(radius); });
}

size_t encodeGwInfo(uint8_t *buf, size_t cap, uint8_t gwId)
{
    return emit(buf, cap, kGwInfo, [&](Writer &w) { w.u8(gwId); });
}

size_t encodeConnect(uint8_t *buf, size_t cap, const Connect &c)
{
    return emit(buf, cap, kConnect, [&](Writer &w) {
        w.u8(c.flags.encode());
        w.u8(c.protocolId);
        w.u16(c.duration);
        w.raw(c.clientId);
    });
}

size_t encodeConnack(uint8_t *buf, size_t cap, uint8_t rc)
{
    return emit(buf, cap, kConnack, [&](Writer &w) { w.u8(rc); });
}

size_t encodeWillTopic(uint8_t *buf, size_t cap, uint8_t type, const WillTopic &t)
{
    return emit(buf, cap, type, [&](Writer &w) {
        if (!t.present)
            return;
        w.u8(t.flags.encode());
        w.raw(t.topic);
    });
}

size_t encodePublish(uint8_t *buf, size_t cap, const Publish &p)
{
    return emit(buf, cap, kPublish, [&](Writer &w) {
        w.u8(p.flags.encode());
        w.u16(p.topicId);
        w.u16(p.msgId);
        w.raw(p.data);
    });
}

size_t encodeRegister(uint8_t *buf, size_t cap, const Register &r)
{
    return emit(buf, cap, kRegister, [&](Writer &w) {
        w.u16(r.topicId);
        w.u16(r.msgId);
        w.raw(r.topicName);
    });
}

size_t encodeTopicAck(uint8_t *buf, size_t cap, uint8_t type, const TopicAck &a)
{
    return emit(buf, cap, type, [&](Writer &w) {
        w.u16(a.topicId);
        w.u16(a.msgId);
        w.u8(a.rc);
    });
}

size_t encodeSubscribe(uint8_t *buf, size_t cap, uint8_t type, const Subscribe &s)
{
    return emit(buf, cap, type, [&](Writer &w) {
        w.u8(s.flags.encode());
        w.u16(s.msgId);
        if (s.flags.topicType == kTopicNormal)
            w.raw(s.topicName);
        else
            w.u16(s.topicId);
    });
}

size_t encodeSuback(uint8_t *buf, size_t cap, const Suback &s)
{
    return emit(buf, cap, kSuback, [&](Writer &w) {
        w.u8(s.flags.encode());
        w.u16(s.topicId);
        w.u16(s.msgId);
        w.u8(s.rc);
    });
}

size_t encodeMsgId(uint8_t *buf, size_t cap, uint8_t type, uint16_t msgId)
{
    return emit(buf, cap, type, [&](Writer &w) { w.u16(msgId); });
}

size_t encodeDisconnect(uint8_t *buf, size_t cap, bool withDuration, uint16_t duration)
{
    return emit(buf, cap, kDisconnect, [&](Writer &w) {
        if (withDuration)
            w.u16(duration);
    });
}

size_t encodePingreq(uint8_t *buf, size_t cap, Bytes clientId)
{
    return emit(buf, cap, kPingreq, [&](Writer &w) { w.raw(clientId); });
}

size_t encodeEmpty(uint8_t *buf, size_t cap, uint8_t type)
{
    return emit(buf, cap, type, [](Writer &) {});
}

size_t encodeRaw(uint8_t *buf, size_t cap, uint8_t type, Bytes body)
{
    return emit(buf, cap, type, [&](Writer &w) { w.raw(body); });
}

}  // namespace sn
}  // namespace tinymq
