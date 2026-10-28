#include "mq_codec.h"

namespace tinymq {

static const uint32_t kMaxVarint = 268435455;

// ---- Reader

Bytes Reader::take(size_t n)
{
    if (!ok_ || n > len_ - pos_) {
        ok_ = false;
        return Bytes();
    }
    Bytes b(p_ + pos_, (uint32_t)n);
    pos_ += n;
    return b;
}

uint8_t Reader::u8()
{
    Bytes b = take(1);
    return ok_ ? b.data[0] : 0;
}

uint16_t Reader::u16()
{
    Bytes b = take(2);
    return ok_ ? (uint16_t)((b.data[0] << 8) | b.data[1]) : 0;
}

uint32_t Reader::u32()
{
    Bytes b = take(4);
    return ok_ ? ((uint32_t)b.data[0] << 24) | ((uint32_t)b.data[1] << 16) | ((uint32_t)b.data[2] << 8) | b.data[3] : 0;
}

uint32_t Reader::varint()
{
    uint32_t value = 0;
    for (int i = 0; i < 4; ++i) {
        uint8_t b = u8();
        if (!ok_)
            return 0;
        value |= (uint32_t)(b & 0x7F) << (7 * i);
        if ((b & 0x80) == 0)
            return value;
    }
    ok_ = false;
    return 0;
}

Bytes Reader::bin()
{
    uint16_t n = u16();
    return take(n);
}

Bytes Reader::str()
{
    Bytes s = bin();
    if (ok_ && !validUtf8(s.data, s.len))
        ok_ = false;
    return ok_ ? s : Bytes();
}

Bytes Reader::rest()
{
    return take(remaining());
}

// ---- Writer

void Writer::raw(const uint8_t *d, size_t n)
{
    if (!ok_)
        return;
    if (buf_) {
        if (n > cap_ - pos_) {
            ok_ = false;
            return;
        }
        if (n)
            memmove(buf_ + pos_, d, n);  // the source may lie further on in the same buffer
    }
    pos_ += n;
}

void Writer::u8(uint8_t v)
{
    raw(&v, 1);
}

void Writer::u16(uint16_t v)
{
    uint8_t b[2] = {(uint8_t)(v >> 8), (uint8_t)v};
    raw(b, 2);
}

void Writer::u32(uint32_t v)
{
    uint8_t b[4] = {(uint8_t)(v >> 24), (uint8_t)(v >> 16), (uint8_t)(v >> 8), (uint8_t)v};
    raw(b, 4);
}

void Writer::varint(uint32_t v)
{
    if (v > kMaxVarint) {
        ok_ = false;
        return;
    }
    do {
        uint8_t b = v & 0x7F;
        v >>= 7;
        if (v)
            b |= 0x80;
        u8(b);
    } while (v);
}

void Writer::str(Bytes s)
{
    if (s.len > 0xFFFF) {
        ok_ = false;
        return;
    }
    u16((uint16_t)s.len);
    raw(s);
}

size_t varintSize(uint32_t v)
{
    return v < 128 ? 1 : v < 16384 ? 2 : v < 2097152 ? 3 : 4;
}

bool validUtf8(const uint8_t *s, size_t n)
{
    size_t i = 0;
    while (i < n) {
        uint8_t c = s[i];
        if (c == 0)
            return false;
        if (c < 0x80) {
            ++i;
            continue;
        }
        size_t more;
        uint8_t lo = 0x80, hi = 0xBF;  // allowed range of the second byte
        if (c < 0xC2)
            return false;  // stray continuation or overlong 2-byte form
        else if (c < 0xE0)
            more = 1;
        else if (c < 0xF0) {
            more = 2;
            if (c == 0xE0)
                lo = 0xA0;  // overlong
            else if (c == 0xED)
                hi = 0x9F;  // surrogates
        } else if (c < 0xF5) {
            more = 3;
            if (c == 0xF0)
                lo = 0x90;  // overlong
            else if (c == 0xF4)
                hi = 0x8F;  // above U+10FFFF
        } else
            return false;
        if (n - i - 1 < more)
            return false;
        if (s[i + 1] < lo || s[i + 1] > hi)
            return false;
        for (size_t k = 2; k <= more; ++k)
            if ((s[i + k] & 0xC0) != 0x80)
                return false;
        i += more + 1;
    }
    return true;
}

// ---- fixed header

HeaderResult parseFixedHeader(const uint8_t *data, size_t len, FixedHeader &out)
{
    if (len < 2)
        return HeaderResult::NeedMore;
    uint8_t type = data[0] >> 4;
    uint8_t flags = data[0] & 0x0F;
    if (type == 0)
        return HeaderResult::Malformed;
    switch ((PacketType)type) {
    case PacketType::Publish:
        if (((flags >> 1) & 3) == 3)
            return HeaderResult::Malformed;
        break;
    case PacketType::Pubrel:
    case PacketType::Subscribe:
    case PacketType::Unsubscribe:
        if (flags != 2)
            return HeaderResult::Malformed;
        break;
    default:
        if (flags != 0)
            return HeaderResult::Malformed;
        break;
    }
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
        if (1 + i >= len)
            return HeaderResult::NeedMore;
        uint8_t b = data[1 + i];
        value |= (uint32_t)(b & 0x7F) << (7 * i);
        if ((b & 0x80) == 0) {
            out.type = (PacketType)type;
            out.flags = flags;
            out.remaining = value;
            out.size = (uint8_t)(2 + i);
            return HeaderResult::Ok;
        }
    }
    return HeaderResult::Malformed;
}

// ---- decoding

static Bytes readProps(Reader &r)
{
    uint32_t n = r.varint();
    return r.take(n);
}

static uint8_t finish(const Reader &r)
{
    return r.atEnd() ? kSuccess : kMalformedPacket;
}

uint8_t decodeConnect(const uint8_t *body, size_t len, Connect &out)
{
    Reader r(body, len);
    Bytes name = r.str();
    uint8_t level = r.u8();
    if (!r.ok())
        return kMalformedPacket;
    bool isMqtt = name.equals("MQTT");
    bool isMqisdp = name.equals("MQIsdp");
    if (!isMqtt && !isMqisdp)
        return kProtocolError;
    out.version = level;
    if (!((isMqtt && (level == kMqtt311 || level == kMqtt5)) || (isMqisdp && level == kMqtt31)))
        return kUnsupportedProtocolVersion;

    uint8_t flags = r.u8();
    if (flags & 0x01)
        return kMalformedPacket;
    out.cleanStart = flags & 0x02;
    out.hasWill = flags & 0x04;
    out.willQos = (flags >> 3) & 3;
    out.willRetain = flags & 0x20;
    out.hasPassword = flags & 0x40;
    out.hasUsername = flags & 0x80;
    if (out.willQos == 3)
        return kMalformedPacket;
    if (!out.hasWill && (out.willQos || out.willRetain))
        return kMalformedPacket;
    if (level != kMqtt5 && out.hasPassword && !out.hasUsername)
        return kMalformedPacket;

    out.keepAlive = r.u16();
    out.props = level == kMqtt5 ? readProps(r) : Bytes();
    out.clientId = r.str();
    if (out.hasWill) {
        out.willProps = level == kMqtt5 ? readProps(r) : Bytes();
        out.willTopic = r.str();
        out.willPayload = r.bin();
    } else {
        out.willProps = out.willTopic = out.willPayload = Bytes();
    }
    out.username = out.hasUsername ? r.str() : Bytes();
    out.password = out.hasPassword ? r.bin() : Bytes();
    return finish(r);
}

uint8_t decodeConnack(const uint8_t *body, size_t len, uint8_t version, Connack &out)
{
    Reader r(body, len);
    uint8_t flags = r.u8();
    uint8_t code = r.u8();
    if (!r.ok() || (flags & 0xFE))
        return kMalformedPacket;
    out.sessionPresent = flags & 1;
    if (version == kMqtt5) {
        out.reason = code;
        out.props = r.remaining() ? readProps(r) : Bytes();
    } else {
        out.reason = reasonFromConnackCode(code);
        out.props = Bytes();
    }
    return finish(r);
}

uint8_t decodePublish(const FixedHeader &h, const uint8_t *body, size_t len, uint8_t version, Publish &out)
{
    Reader r(body, len);
    out.qos = (h.flags >> 1) & 3;
    out.dup = h.flags & 0x08;
    out.retain = h.flags & 0x01;
    if (version == kMqtt5 && out.qos == 0 && out.dup)
        return kMalformedPacket;
    out.topic = r.str();
    out.packetId = 0;
    if (out.qos) {
        out.packetId = r.u16();
        if (r.ok() && out.packetId == 0)
            return kMalformedPacket;
    }
    out.props = version == kMqtt5 ? readProps(r) : Bytes();
    out.payload = r.rest();
    return r.ok() ? kSuccess : kMalformedPacket;
}

uint8_t decodeAck(const FixedHeader &h, const uint8_t *body, size_t len, uint8_t version, Ack &out)
{
    (void)h;
    Reader r(body, len);
    out.packetId = r.u16();
    out.reason = kSuccess;
    out.props = Bytes();
    if (!r.ok() || out.packetId == 0)
        return kMalformedPacket;
    if (version == kMqtt5) {
        if (r.remaining())
            out.reason = r.u8();
        if (r.remaining())
            out.props = readProps(r);
    }
    return finish(r);
}

static bool validSubOptions(uint8_t b, uint8_t version)
{
    if ((b & 3) == 3)
        return false;
    if (version == kMqtt5)
        return (b & 0xC0) == 0 && ((b >> 4) & 3) != 3;
    return (b & 0xFC) == 0;
}

uint8_t decodeSubscribe(const uint8_t *body, size_t len, uint8_t version, Subscribe &out)
{
    Reader r(body, len);
    out.packetId = r.u16();
    if (r.ok() && out.packetId == 0)
        return kMalformedPacket;
    out.props = version == kMqtt5 ? readProps(r) : Bytes();
    out.entries = r.rest();
    if (!r.ok() || out.entries.len == 0)
        return kMalformedPacket;
    Reader e(out.entries.data, out.entries.len);
    while (e.remaining()) {
        e.str();
        uint8_t opts = e.u8();
        if (!e.ok() || !validSubOptions(opts, version))
            return kMalformedPacket;
    }
    return kSuccess;
}

uint8_t decodeUnsubscribe(const uint8_t *body, size_t len, uint8_t version, Unsubscribe &out)
{
    Reader r(body, len);
    out.packetId = r.u16();
    if (r.ok() && out.packetId == 0)
        return kMalformedPacket;
    out.props = version == kMqtt5 ? readProps(r) : Bytes();
    out.entries = r.rest();
    if (!r.ok() || out.entries.len == 0)
        return kMalformedPacket;
    Reader e(out.entries.data, out.entries.len);
    while (e.remaining())
        e.str();
    return e.ok() ? kSuccess : kMalformedPacket;
}

uint8_t decodeSubAck(const FixedHeader &h, const uint8_t *body, size_t len, uint8_t version, SubAck &out)
{
    Reader r(body, len);
    out.packetId = r.u16();
    if (r.ok() && out.packetId == 0)
        return kMalformedPacket;
    out.props = version == kMqtt5 ? readProps(r) : Bytes();
    out.reasons = r.rest();
    if (!r.ok())
        return kMalformedPacket;
    if (h.type == PacketType::Suback || version == kMqtt5)
        return out.reasons.len ? kSuccess : kMalformedPacket;
    return out.reasons.len ? kMalformedPacket : kSuccess;  // 3.1.1 UNSUBACK has no payload
}

uint8_t decodeDisconnect(const FixedHeader &h, const uint8_t *body, size_t len, uint8_t version, Disconnect &out)
{
    Reader r(body, len);
    out.reason = kSuccess;
    out.props = Bytes();
    if (version != kMqtt5)
        return h.type == PacketType::Auth ? (uint8_t)kProtocolError : finish(r);
    if (r.remaining())
        out.reason = r.u8();
    if (r.remaining())
        out.props = readProps(r);
    return finish(r);
}

uint8_t SubOptions::encode() const
{
    return (uint8_t)((qos & 3) | (noLocal ? 0x04 : 0) | (retainAsPublished ? 0x08 : 0) | ((retainHandling & 3) << 4));
}

SubOptions SubOptions::decode(uint8_t b)
{
    SubOptions o;
    o.qos = b & 3;
    o.noLocal = b & 0x04;
    o.retainAsPublished = b & 0x08;
    o.retainHandling = (b >> 4) & 3;
    return o;
}

bool nextSubscription(const Subscribe &s, size_t &pos, Bytes &filter, SubOptions &opts)
{
    if (pos >= s.entries.len)
        return false;
    Reader r(s.entries.data + pos, s.entries.len - pos);
    filter = r.str();
    opts = SubOptions::decode(r.u8());
    if (!r.ok())
        return false;
    pos = s.entries.len - r.remaining();
    return true;
}

bool nextUnsubscription(const Unsubscribe &u, size_t &pos, Bytes &filter)
{
    if (pos >= u.entries.len)
        return false;
    Reader r(u.entries.data + pos, u.entries.len - pos);
    filter = r.str();
    if (!r.ok())
        return false;
    pos = u.entries.len - r.remaining();
    return true;
}

// ---- encoding

// Runs `body` once to count, then writes the fixed header and the body.
// `extra` is counted in the remaining length but not written (the payload
// of encodePublishHead()).
template <typename F>
static size_t emit(uint8_t *buf, size_t cap, uint8_t first, F body, uint32_t extra = 0)
{
    Writer counter(nullptr, 0);
    body(counter);
    if (!counter.ok() || counter.pos() + extra > kMaxVarint)
        return 0;
    Writer w(buf, cap);
    w.u8(first);
    w.varint((uint32_t)(counter.pos() + extra));
    body(w);
    return w.ok() ? w.pos() : 0;
}

static void writeProps(Writer &w, Bytes props)
{
    w.varint(props.len);
    w.raw(props);
}

size_t encodeConnect(uint8_t *buf, size_t cap, const Connect &c)
{
    return emit(buf, cap, 0x10, [&](Writer &w) {
        w.str(Bytes(c.version == kMqtt31 ? "MQIsdp" : "MQTT"));
        w.u8(c.version);
        uint8_t flags = 0;
        if (c.cleanStart)
            flags |= 0x02;
        if (c.hasWill)
            flags |= 0x04 | ((c.willQos & 3) << 3) | (c.willRetain ? 0x20 : 0);
        if (c.hasPassword)
            flags |= 0x40;
        if (c.hasUsername)
            flags |= 0x80;
        w.u8(flags);
        w.u16(c.keepAlive);
        if (c.version == kMqtt5)
            writeProps(w, c.props);
        w.str(c.clientId);
        if (c.hasWill) {
            if (c.version == kMqtt5)
                writeProps(w, c.willProps);
            w.str(c.willTopic);
            w.str(c.willPayload);
        }
        if (c.hasUsername)
            w.str(c.username);
        if (c.hasPassword)
            w.str(c.password);
    });
}

size_t encodeConnack(uint8_t *buf, size_t cap, uint8_t version, const Connack &c)
{
    return emit(buf, cap, 0x20, [&](Writer &w) {
        w.u8(c.sessionPresent ? 1 : 0);
        if (version == kMqtt5) {
            w.u8(c.reason);
            writeProps(w, c.props);
        } else {
            w.u8(connackCodeFromReason(c.reason));
        }
    });
}

static uint8_t publishFirstByte(const Publish &p)
{
    return (uint8_t)(0x30 | (p.dup ? 0x08 : 0) | ((p.qos & 3) << 1) | (p.retain ? 0x01 : 0));
}

static void publishHead(Writer &w, uint8_t version, const Publish &p)
{
    w.str(p.topic);
    if (p.qos)
        w.u16(p.packetId);
    if (version == kMqtt5)
        writeProps(w, p.props);
}

size_t encodePublish(uint8_t *buf, size_t cap, uint8_t version, const Publish &p)
{
    return emit(buf, cap, publishFirstByte(p), [&](Writer &w) {
        publishHead(w, version, p);
        w.raw(p.payload);
    });
}

size_t encodePublishHead(uint8_t *buf, size_t cap, uint8_t version, const Publish &p)
{
    return emit(buf, cap, publishFirstByte(p), [&](Writer &w) { publishHead(w, version, p); }, p.payload.len);
}

size_t encodeAck(uint8_t *buf, size_t cap, uint8_t version, PacketType type, const Ack &a)
{
    uint8_t first = (uint8_t)((uint8_t)type << 4 | (type == PacketType::Pubrel ? 2 : 0));
    return emit(buf, cap, first, [&](Writer &w) {
        w.u16(a.packetId);
        if (version == kMqtt5 && (a.reason != kSuccess || a.props.len)) {
            w.u8(a.reason);
            if (a.props.len)
                writeProps(w, a.props);
        }
    });
}

size_t encodeSubscribe(uint8_t *buf, size_t cap, uint8_t version, const Subscribe &s)
{
    return emit(buf, cap, 0x82, [&](Writer &w) {
        w.u16(s.packetId);
        if (version == kMqtt5)
            writeProps(w, s.props);
        w.raw(s.entries);
    });
}

size_t encodeUnsubscribe(uint8_t *buf, size_t cap, uint8_t version, const Unsubscribe &u)
{
    return emit(buf, cap, 0xA2, [&](Writer &w) {
        w.u16(u.packetId);
        if (version == kMqtt5)
            writeProps(w, u.props);
        w.raw(u.entries);
    });
}

size_t encodeSubAck(uint8_t *buf, size_t cap, uint8_t version, PacketType type, const SubAck &a)
{
    return emit(buf, cap, (uint8_t)((uint8_t)type << 4), [&](Writer &w) {
        w.u16(a.packetId);
        if (version == kMqtt5)
            writeProps(w, a.props);
        if (type == PacketType::Suback || version == kMqtt5)
            w.raw(a.reasons);
    });
}

size_t encodePingreq(uint8_t *buf, size_t cap)
{
    return emit(buf, cap, 0xC0, [](Writer &) {});
}

size_t encodePingresp(uint8_t *buf, size_t cap)
{
    return emit(buf, cap, 0xD0, [](Writer &) {});
}

size_t encodeDisconnect(uint8_t *buf, size_t cap, uint8_t version, PacketType type, const Disconnect &d)
{
    return emit(buf, cap, (uint8_t)((uint8_t)type << 4), [&](Writer &w) {
        if (version == kMqtt5 && (d.reason != kSuccess || d.props.len || type == PacketType::Auth)) {
            w.u8(d.reason);
            if (d.props.len)
                writeProps(w, d.props);
        }
    });
}

size_t appendSubscription(uint8_t *buf, size_t cap, size_t pos, Bytes filter, SubOptions opts)
{
    if (pos > cap)
        return 0;
    Writer w(buf + pos, cap - pos);
    w.str(filter);
    w.u8(opts.encode());
    return w.ok() ? pos + w.pos() : 0;
}

size_t appendUnsubscription(uint8_t *buf, size_t cap, size_t pos, Bytes filter)
{
    if (pos > cap)
        return 0;
    Writer w(buf + pos, cap - pos);
    w.str(filter);
    return w.ok() ? pos + w.pos() : 0;
}

uint8_t connackCodeFromReason(uint8_t reason)
{
    switch (reason) {
    case kSuccess:
        return 0;
    case kUnsupportedProtocolVersion:
        return 1;
    case kClientIdentifierNotValid:
        return 2;
    case kBadUserNameOrPassword:
        return 4;
    case kNotAuthorized:
    case kBanned:
        return 5;
    default:
        return 3;  // server unavailable
    }
}

uint8_t reasonFromConnackCode(uint8_t code)
{
    switch (code) {
    case 0:
        return kSuccess;
    case 1:
        return kUnsupportedProtocolVersion;
    case 2:
        return kClientIdentifierNotValid;
    case 3:
        return kServerUnavailable;
    case 4:
        return kBadUserNameOrPassword;
    case 5:
        return kNotAuthorized;
    default:
        return kUnspecifiedError;
    }
}

}  // namespace tinymq
