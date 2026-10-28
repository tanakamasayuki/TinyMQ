// Codec unit tests: primitives, fixed header, and every packet type in
// 3.1.1 and 5.0, checked against hand-built byte sequences from the
// specifications, plus malformed packets that must be rejected.

#include <TinyMQ.h>
#include "expect.h"

using namespace tinymq;

static uint8_t g_buf[512];

#define BYTES(...) \
    static const uint8_t want[] = {__VA_ARGS__}

// Decodes a whole packet (fixed header included) into its header and body
static bool split(const uint8_t *pkt, size_t len, FixedHeader &h, const uint8_t *&body)
{
    if (parseFixedHeader(pkt, len, h) != HeaderResult::Ok)
        return false;
    body = pkt + h.size;
    return h.size + h.remaining == len;
}

static void testVarint()
{
    static const uint32_t values[] = {0, 127, 128, 16383, 16384, 2097151, 2097152, 268435455};
    bool ok = true;
    for (uint32_t v : values) {
        Writer w(g_buf, sizeof(g_buf));
        w.varint(v);
        Reader r(g_buf, w.pos());
        if (r.varint() != v || !r.atEnd() || w.pos() != varintSize(v))
            ok = false;
    }
    EXPECT_TRUE("varint round trip at the boundaries", ok);

    Writer w(g_buf, sizeof(g_buf));
    w.varint(268435456);
    EXPECT_TRUE("varint above the maximum fails", !w.ok());

    static const uint8_t five[] = {0x80, 0x80, 0x80, 0x80, 0x01};
    Reader r(five, sizeof(five));
    r.varint();
    EXPECT_TRUE("varint of five bytes is rejected", !r.ok());
}

static void testUtf8()
{
    static const uint8_t good[] = {'a', 0xC3, 0xA9, 0xE6, 0x97, 0xA5, 0xF0, 0x9D, 0x84, 0x9E};
    EXPECT_TRUE("utf8 valid mixed", validUtf8(good, sizeof(good)));
    static const uint8_t nul[] = {'a', 0x00};
    EXPECT_TRUE("utf8 U+0000 rejected", !validUtf8(nul, sizeof(nul)));
    static const uint8_t overlong[] = {0xC0, 0x80};
    EXPECT_TRUE("utf8 overlong rejected", !validUtf8(overlong, sizeof(overlong)));
    static const uint8_t overlong3[] = {0xE0, 0x80, 0x80};
    EXPECT_TRUE("utf8 overlong 3-byte rejected", !validUtf8(overlong3, sizeof(overlong3)));
    static const uint8_t surrogate[] = {0xED, 0xA0, 0x80};
    EXPECT_TRUE("utf8 surrogate rejected", !validUtf8(surrogate, sizeof(surrogate)));
    static const uint8_t tooBig[] = {0xF4, 0x90, 0x80, 0x80};
    EXPECT_TRUE("utf8 above U+10FFFF rejected", !validUtf8(tooBig, sizeof(tooBig)));
    static const uint8_t truncated[] = {0xE3, 0x81};
    EXPECT_TRUE("utf8 truncated rejected", !validUtf8(truncated, sizeof(truncated)));
    static const uint8_t stray[] = {0x80};
    EXPECT_TRUE("utf8 stray continuation rejected", !validUtf8(stray, sizeof(stray)));
    static const uint8_t badCont[] = {0xE3, 0x81, 0x41};
    EXPECT_TRUE("utf8 bad continuation rejected", !validUtf8(badCont, sizeof(badCont)));
}

static void testFixedHeader()
{
    FixedHeader h;
    static const uint8_t one[] = {0x30};
    EXPECT_TRUE("header needs two bytes", parseFixedHeader(one, 1, h) == HeaderResult::NeedMore);
    static const uint8_t partial[] = {0x30, 0x80};
    EXPECT_TRUE("header waits for the rest of the length", parseFixedHeader(partial, 2, h) == HeaderResult::NeedMore);
    static const uint8_t big[] = {0x30, 0xFF, 0xFF, 0xFF, 0x7F};
    EXPECT_TRUE("header maximum length", parseFixedHeader(big, 5, h) == HeaderResult::Ok && h.remaining == 268435455 && h.size == 5);
    static const uint8_t tooLong[] = {0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01};
    EXPECT_TRUE("header five length bytes rejected", parseFixedHeader(tooLong, 6, h) == HeaderResult::Malformed);

    static const uint8_t pubrelBad[] = {0x60, 0x02};
    EXPECT_TRUE("PUBREL flags must be 0010", parseFixedHeader(pubrelBad, 2, h) == HeaderResult::Malformed);
    static const uint8_t pubrelOk[] = {0x62, 0x02};
    EXPECT_TRUE("PUBREL with 0010", parseFixedHeader(pubrelOk, 2, h) == HeaderResult::Ok && h.type == PacketType::Pubrel);
    static const uint8_t subBad[] = {0x80, 0x00};
    EXPECT_TRUE("SUBSCRIBE flags must be 0010", parseFixedHeader(subBad, 2, h) == HeaderResult::Malformed);
    static const uint8_t qos3[] = {0x36, 0x00};
    EXPECT_TRUE("PUBLISH QoS 3 rejected", parseFixedHeader(qos3, 2, h) == HeaderResult::Malformed);
    static const uint8_t connFlags[] = {0x11, 0x00};
    EXPECT_TRUE("CONNECT with flags rejected", parseFixedHeader(connFlags, 2, h) == HeaderResult::Malformed);
    static const uint8_t type0[] = {0x00, 0x00};
    EXPECT_TRUE("packet type 0 rejected", parseFixedHeader(type0, 2, h) == HeaderResult::Malformed);
}

static void testConnect()
{
    Connect c;
    c.clientId = Bytes("abc");
    c.keepAlive = 60;
    size_t n = encodeConnect(g_buf, sizeof(g_buf), c);
    BYTES(0x10, 0x0F, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x03, 'a', 'b', 'c');
    expectBytes("CONNECT 3.1.1 minimal bytes", g_buf, n, want, sizeof(want));

    // full round trip, 3.1.1
    Connect full;
    full.version = kMqtt311;
    full.cleanStart = false;
    full.keepAlive = 300;
    full.clientId = Bytes("dev-1");
    full.hasWill = true;
    full.willQos = 2;
    full.willRetain = true;
    full.willTopic = Bytes("dev/1/status");
    full.willPayload = Bytes("offline");
    full.hasUsername = true;
    full.username = Bytes("user");
    full.hasPassword = true;
    full.password = Bytes("pa\x01ss");
    n = encodeConnect(g_buf, sizeof(g_buf), full);
    FixedHeader h;
    const uint8_t *body;
    Connect d;
    bool ok = split(g_buf, n, h, body) && h.type == PacketType::Connect && decodeConnect(body, h.remaining, d) == kSuccess;
    EXPECT_TRUE("CONNECT 3.1.1 full decodes", ok);
    EXPECT_TRUE("CONNECT 3.1.1 fields", ok && d.version == kMqtt311 && !d.cleanStart && d.keepAlive == 300 && d.clientId.equals("dev-1") && d.hasWill && d.willQos == 2 && d.willRetain && d.willTopic.equals("dev/1/status") && d.willPayload.equals("offline") && d.hasUsername && d.username.equals("user") && d.hasPassword && d.password.equals("pa\x01ss"));

    // 5.0 with properties: Session Expiry Interval = 10
    static const uint8_t props[] = {0x11, 0x00, 0x00, 0x00, 0x0A};
    static const uint8_t willProps[] = {0x18, 0x00, 0x00, 0x00, 0x05};
    full.version = kMqtt5;
    full.props = Bytes(props, sizeof(props));
    full.willProps = Bytes(willProps, sizeof(willProps));
    full.hasUsername = false;  // allowed without a user name in 5.0
    n = encodeConnect(g_buf, sizeof(g_buf), full);
    ok = split(g_buf, n, h, body) && decodeConnect(body, h.remaining, d) == kSuccess;
    EXPECT_TRUE("CONNECT 5.0 decodes", ok);
    EXPECT_TRUE("CONNECT 5.0 properties", ok && d.version == kMqtt5 && d.props.len == sizeof(props) && memcmp(d.props.data, props, sizeof(props)) == 0 && d.willProps.len == sizeof(willProps) && d.willTopic.equals("dev/1/status") && !d.hasUsername && d.password.equals("pa\x01ss"));

    // 3.1
    c.version = kMqtt31;
    n = encodeConnect(g_buf, sizeof(g_buf), c);
    ok = split(g_buf, n, h, body) && decodeConnect(body, h.remaining, d) == kSuccess && d.version == kMqtt31;
    EXPECT_TRUE("CONNECT 3.1 (MQIsdp) decodes", ok);

    // rejections: bodies after the fixed header
    {
        static const uint8_t b[] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x03, 0x00, 0x3C, 0x00, 0x01, 'a'};
        EXPECT_EQ("CONNECT reserved flag rejected", decodeConnect(b, sizeof(b), d), kMalformedPacket);
    }
    {
        static const uint8_t b[] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x0A, 0x00, 0x3C, 0x00, 0x01, 'a'};
        EXPECT_EQ("CONNECT will QoS without will flag rejected", decodeConnect(b, sizeof(b), d), kMalformedPacket);
    }
    {
        static const uint8_t b[] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x42, 0x00, 0x3C, 0x00, 0x01, 'a', 0x00, 0x01, 'p'};
        EXPECT_EQ("CONNECT 3.1.1 password without user name rejected", decodeConnect(b, sizeof(b), d), kMalformedPacket);
    }
    {
        static const uint8_t b[] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x06, 0x02, 0x00, 0x3C, 0x00, 0x01, 'a'};
        EXPECT_TRUE("CONNECT level 6 is unsupported", decodeConnect(b, sizeof(b), d) == kUnsupportedProtocolVersion && d.version == 6);
    }
    {
        static const uint8_t b[] = {0x00, 0x04, 'M', 'Q', 'T', 'X', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x01, 'a'};
        EXPECT_EQ("CONNECT wrong protocol name", decodeConnect(b, sizeof(b), d), kProtocolError);
    }
    {
        static const uint8_t b[] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x01, 'a', 0x00};
        EXPECT_EQ("CONNECT trailing byte rejected", decodeConnect(b, sizeof(b), d), kMalformedPacket);
    }
    {
        static const uint8_t b[] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x02, 'a'};
        EXPECT_EQ("CONNECT truncated client id rejected", decodeConnect(b, sizeof(b), d), kMalformedPacket);
    }
    {
        static const uint8_t b[] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x02, 0xC0, 0x80};
        EXPECT_EQ("CONNECT client id with bad UTF-8 rejected", decodeConnect(b, sizeof(b), d), kMalformedPacket);
    }
    {
        static const uint8_t b[] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x00};
        EXPECT_TRUE("CONNECT empty client id accepted by the codec", decodeConnect(b, sizeof(b), d) == kSuccess && d.clientId.len == 0);
    }
}

static void testConnack()
{
    Connack c;
    size_t n = encodeConnack(g_buf, sizeof(g_buf), kMqtt311, c);
    {
        BYTES(0x20, 0x02, 0x00, 0x00);
        expectBytes("CONNACK 3.1.1 accepted", g_buf, n, want, sizeof(want));
    }
    c.reason = kBadUserNameOrPassword;
    n = encodeConnack(g_buf, sizeof(g_buf), kMqtt311, c);
    {
        BYTES(0x20, 0x02, 0x00, 0x04);
        expectBytes("CONNACK 3.1.1 bad user name maps to 4", g_buf, n, want, sizeof(want));
    }
    c.reason = kSuccess;
    c.sessionPresent = true;
    n = encodeConnack(g_buf, sizeof(g_buf), kMqtt5, c);
    {
        BYTES(0x20, 0x03, 0x01, 0x00, 0x00);
        expectBytes("CONNACK 5.0 session present, no properties", g_buf, n, want, sizeof(want));
    }

    Connack d;
    {
        static const uint8_t b[] = {0x00, 0x05};
        EXPECT_TRUE("CONNACK 3.1.1 code 5 is not authorized", decodeConnack(b, 2, kMqtt311, d) == kSuccess && d.reason == kNotAuthorized);
    }
    {
        static const uint8_t b[] = {0x02, 0x00};
        EXPECT_EQ("CONNACK reserved flag bits rejected", decodeConnack(b, 2, kMqtt311, d), kMalformedPacket);
    }
    {
        static const uint8_t b[] = {0x00, 0x00, 0x03, 0x21, 0x00, 0x0A};  // Receive Maximum 10
        EXPECT_TRUE("CONNACK 5.0 with properties", decodeConnack(b, sizeof(b), kMqtt5, d) == kSuccess && d.props.len == 3 && d.props.data[0] == 0x21);
    }
}

static void testPublish()
{
    Publish p;
    p.topic = Bytes("a/b");
    p.payload = Bytes("hi");
    size_t n = encodePublish(g_buf, sizeof(g_buf), kMqtt311, p);
    {
        BYTES(0x30, 0x07, 0x00, 0x03, 'a', '/', 'b', 'h', 'i');
        expectBytes("PUBLISH QoS 0", g_buf, n, want, sizeof(want));
    }
    p.qos = 1;
    p.retain = true;
    p.packetId = 10;
    n = encodePublish(g_buf, sizeof(g_buf), kMqtt311, p);
    {
        BYTES(0x33, 0x09, 0x00, 0x03, 'a', '/', 'b', 0x00, 0x0A, 'h', 'i');
        expectBytes("PUBLISH QoS 1 retain", g_buf, n, want, sizeof(want));
    }
    size_t head = encodePublishHead(g_buf, sizeof(g_buf), kMqtt311, p);
    EXPECT_TRUE("PUBLISH head stops before the payload", head == n - 2 && g_buf[1] == 0x09);

    p.qos = 2;
    p.dup = true;
    p.retain = false;
    n = encodePublish(g_buf, sizeof(g_buf), kMqtt5, p);
    {
        BYTES(0x3C, 0x0A, 0x00, 0x03, 'a', '/', 'b', 0x00, 0x0A, 0x00, 'h', 'i');
        expectBytes("PUBLISH 5.0 QoS 2 dup, empty properties", g_buf, n, want, sizeof(want));
    }
    FixedHeader h;
    const uint8_t *body;
    Publish d;
    bool ok = split(g_buf, n, h, body) && decodePublish(h, body, h.remaining, kMqtt5, d) == kSuccess;
    EXPECT_TRUE("PUBLISH 5.0 decodes", ok && d.qos == 2 && d.dup && !d.retain && d.topic.equals("a/b") && d.packetId == 10 && d.props.len == 0 && d.payload.equals("hi"));

    // empty payload
    p.qos = 0;
    p.dup = false;
    p.payload = Bytes();
    n = encodePublish(g_buf, sizeof(g_buf), kMqtt311, p);
    ok = split(g_buf, n, h, body) && decodePublish(h, body, h.remaining, kMqtt311, d) == kSuccess;
    EXPECT_TRUE("PUBLISH empty payload", ok && d.payload.len == 0 && d.topic.equals("a/b"));

    {
        static const uint8_t pkt[] = {0x32, 0x05, 0x00, 0x01, 'a', 0x00, 0x00};
        ok = split(pkt, sizeof(pkt), h, body);
        EXPECT_TRUE("PUBLISH QoS 1 packet id 0 rejected", ok && decodePublish(h, body, h.remaining, kMqtt311, d) == kMalformedPacket);
    }
    {
        static const uint8_t pkt[] = {0x38, 0x04, 0x00, 0x01, 'a', 0x00};
        ok = split(pkt, sizeof(pkt), h, body);
        EXPECT_TRUE("PUBLISH 5.0 QoS 0 with DUP rejected", ok && decodePublish(h, body, h.remaining, kMqtt5, d) == kMalformedPacket);
    }
    {
        static const uint8_t pkt[] = {0x30, 0x03, 0x00, 0x05, 'a'};
        ok = split(pkt, sizeof(pkt), h, body);
        EXPECT_TRUE("PUBLISH truncated topic rejected", ok && decodePublish(h, body, h.remaining, kMqtt311, d) == kMalformedPacket);
    }
    {
        static const uint8_t pkt[] = {0x30, 0x05, 0x00, 0x01, 'a', 0x05, 0x01};
        ok = split(pkt, sizeof(pkt), h, body);
        EXPECT_TRUE("PUBLISH 5.0 property length past the end rejected", ok && decodePublish(h, body, h.remaining, kMqtt5, d) == kMalformedPacket);
    }
}

static void testAcks()
{
    Ack a;
    a.packetId = 10;
    size_t n = encodeAck(g_buf, sizeof(g_buf), kMqtt311, PacketType::Puback, a);
    {
        BYTES(0x40, 0x02, 0x00, 0x0A);
        expectBytes("PUBACK", g_buf, n, want, sizeof(want));
    }
    n = encodeAck(g_buf, sizeof(g_buf), kMqtt311, PacketType::Pubrel, a);
    {
        BYTES(0x62, 0x02, 0x00, 0x0A);
        expectBytes("PUBREL", g_buf, n, want, sizeof(want));
    }
    n = encodeAck(g_buf, sizeof(g_buf), kMqtt5, PacketType::Pubrec, a);
    {
        BYTES(0x50, 0x02, 0x00, 0x0A);
        expectBytes("PUBREC 5.0 success is short", g_buf, n, want, sizeof(want));
    }
    a.reason = kNoMatchingSubscribers;
    n = encodeAck(g_buf, sizeof(g_buf), kMqtt5, PacketType::Puback, a);
    {
        BYTES(0x40, 0x03, 0x00, 0x0A, 0x10);
        expectBytes("PUBACK 5.0 with reason", g_buf, n, want, sizeof(want));
    }

    FixedHeader h;
    Ack d;
    h.type = PacketType::Puback;
    {
        static const uint8_t b[] = {0x00, 0x0A, 0x10, 0x00};
        EXPECT_TRUE("PUBACK 5.0 with reason and empty properties", decodeAck(h, b, sizeof(b), kMqtt5, d) == kSuccess && d.packetId == 10 && d.reason == 0x10);
    }
    {
        static const uint8_t b[] = {0x00, 0x0A, 0x10};
        EXPECT_EQ("PUBACK 3.1.1 extra byte rejected", decodeAck(h, b, sizeof(b), kMqtt311, d), kMalformedPacket);
    }
    {
        static const uint8_t b[] = {0x00, 0x00};
        EXPECT_EQ("PUBACK packet id 0 rejected", decodeAck(h, b, sizeof(b), kMqtt311, d), kMalformedPacket);
    }
}

static void testSubscribe()
{
    static uint8_t entries[64];
    size_t pos = 0;
    SubOptions q1;
    q1.qos = 1;
    pos = appendSubscription(entries, sizeof(entries), pos, Bytes("a/+"), q1);
    pos = appendSubscription(entries, sizeof(entries), pos, Bytes("#"), SubOptions());
    Subscribe s;
    s.packetId = 2;
    s.entries = Bytes(entries, (uint32_t)pos);
    size_t n = encodeSubscribe(g_buf, sizeof(g_buf), kMqtt311, s);
    {
        BYTES(0x82, 0x0C, 0x00, 0x02, 0x00, 0x03, 'a', '/', '+', 0x01, 0x00, 0x01, '#', 0x00);
        expectBytes("SUBSCRIBE two filters", g_buf, n, want, sizeof(want));
    }
    FixedHeader h;
    const uint8_t *body;
    Subscribe d;
    bool ok = split(g_buf, n, h, body) && decodeSubscribe(body, h.remaining, kMqtt311, d) == kSuccess && d.packetId == 2;
    size_t it = 0;
    Bytes f1, f2, f3;
    SubOptions o1, o2, o3;
    ok = ok && nextSubscription(d, it, f1, o1) && nextSubscription(d, it, f2, o2) && !nextSubscription(d, it, f3, o3);
    EXPECT_TRUE("SUBSCRIBE walks its entries", ok && f1.equals("a/+") && o1.qos == 1 && f2.equals("#") && o2.qos == 0);

    {
        static const uint8_t b[] = {0x00, 0x01, 0x00, 0x01, 'a', 0x04};  // No Local
        EXPECT_EQ("SUBSCRIBE 3.1.1 reserved option bits rejected", decodeSubscribe(b, sizeof(b), kMqtt311, d), kMalformedPacket);
        static const uint8_t b5[] = {0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x2D};  // QoS 1, No Local, RAP, Retain Handling 2
        ok = decodeSubscribe(b5, sizeof(b5), kMqtt5, d) == kSuccess;
        it = 0;
        ok = ok && nextSubscription(d, it, f1, o1);
        EXPECT_TRUE("SUBSCRIBE 5.0 options", ok && o1.qos == 1 && o1.noLocal && o1.retainAsPublished && o1.retainHandling == 2);
    }
    {
        static const uint8_t b[] = {0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x30};  // Retain Handling 3
        EXPECT_EQ("SUBSCRIBE 5.0 retain handling 3 rejected", decodeSubscribe(b, sizeof(b), kMqtt5, d), kMalformedPacket);
    }
    {
        static const uint8_t b[] = {0x00, 0x01, 0x00, 0x01, 'a', 0x03};
        EXPECT_EQ("SUBSCRIBE QoS 3 rejected", decodeSubscribe(b, sizeof(b), kMqtt311, d), kMalformedPacket);
    }
    {
        static const uint8_t b[] = {0x00, 0x01};
        EXPECT_EQ("SUBSCRIBE without filters rejected", decodeSubscribe(b, sizeof(b), kMqtt311, d), kMalformedPacket);
    }
    {
        static const uint8_t b[] = {0x00, 0x00, 0x00, 0x01, 'a', 0x00};
        EXPECT_EQ("SUBSCRIBE packet id 0 rejected", decodeSubscribe(b, sizeof(b), kMqtt311, d), kMalformedPacket);
    }
    {
        static const uint8_t b[] = {0x00, 0x01, 0x00, 0x01, 'a'};
        EXPECT_EQ("SUBSCRIBE missing options rejected", decodeSubscribe(b, sizeof(b), kMqtt311, d), kMalformedPacket);
    }

    // UNSUBSCRIBE
    pos = appendUnsubscription(entries, sizeof(entries), 0, Bytes("a/+"));
    Unsubscribe u;
    u.packetId = 3;
    u.entries = Bytes(entries, (uint32_t)pos);
    n = encodeUnsubscribe(g_buf, sizeof(g_buf), kMqtt311, u);
    {
        BYTES(0xA2, 0x07, 0x00, 0x03, 0x00, 0x03, 'a', '/', '+');
        expectBytes("UNSUBSCRIBE", g_buf, n, want, sizeof(want));
    }
    Unsubscribe ud;
    ok = split(g_buf, n, h, body) && decodeUnsubscribe(body, h.remaining, kMqtt311, ud) == kSuccess;
    it = 0;
    ok = ok && nextUnsubscription(ud, it, f1) && !nextUnsubscription(ud, it, f2);
    EXPECT_TRUE("UNSUBSCRIBE walks its entries", ok && f1.equals("a/+"));
}

static void testSubAck()
{
    static const uint8_t codes[] = {0x01, 0x80};
    SubAck a;
    a.packetId = 2;
    a.reasons = Bytes(codes, 2);
    size_t n = encodeSubAck(g_buf, sizeof(g_buf), kMqtt311, PacketType::Suback, a);
    {
        BYTES(0x90, 0x04, 0x00, 0x02, 0x01, 0x80);
        expectBytes("SUBACK", g_buf, n, want, sizeof(want));
    }
    n = encodeSubAck(g_buf, sizeof(g_buf), kMqtt311, PacketType::Unsuback, a);
    {
        BYTES(0xB0, 0x02, 0x00, 0x02);
        expectBytes("UNSUBACK 3.1.1 has no reasons", g_buf, n, want, sizeof(want));
    }
    n = encodeSubAck(g_buf, sizeof(g_buf), kMqtt5, PacketType::Unsuback, a);
    {
        BYTES(0xB0, 0x05, 0x00, 0x02, 0x00, 0x01, 0x80);
        expectBytes("UNSUBACK 5.0 with reasons", g_buf, n, want, sizeof(want));
    }
    FixedHeader h;
    const uint8_t *body;
    SubAck d;
    bool ok = split(g_buf, n, h, body) && decodeSubAck(h, body, h.remaining, kMqtt5, d) == kSuccess;
    EXPECT_TRUE("UNSUBACK 5.0 decodes", ok && d.packetId == 2 && d.reasons.len == 2 && d.reasons.data[1] == 0x80);
    {
        static const uint8_t pkt[] = {0xB0, 0x03, 0x00, 0x02, 0x00};
        ok = split(pkt, sizeof(pkt), h, body);
        EXPECT_TRUE("UNSUBACK 3.1.1 with a payload rejected", ok && decodeSubAck(h, body, h.remaining, kMqtt311, d) == kMalformedPacket);
    }
    {
        static const uint8_t pkt[] = {0x90, 0x02, 0x00, 0x02};
        ok = split(pkt, sizeof(pkt), h, body);
        EXPECT_TRUE("SUBACK without reasons rejected", ok && decodeSubAck(h, body, h.remaining, kMqtt311, d) == kMalformedPacket);
    }
}

static void testSmallPackets()
{
    size_t n = encodePingreq(g_buf, sizeof(g_buf));
    {
        BYTES(0xC0, 0x00);
        expectBytes("PINGREQ", g_buf, n, want, sizeof(want));
    }
    n = encodePingresp(g_buf, sizeof(g_buf));
    {
        BYTES(0xD0, 0x00);
        expectBytes("PINGRESP", g_buf, n, want, sizeof(want));
    }
    Disconnect d;
    n = encodeDisconnect(g_buf, sizeof(g_buf), kMqtt5, PacketType::Disconnect, d);
    {
        BYTES(0xE0, 0x00);
        expectBytes("DISCONNECT 5.0 normal is short", g_buf, n, want, sizeof(want));
    }
    d.reason = kDisconnectWithWill;
    n = encodeDisconnect(g_buf, sizeof(g_buf), kMqtt5, PacketType::Disconnect, d);
    {
        BYTES(0xE0, 0x01, 0x04);
        expectBytes("DISCONNECT 5.0 with will", g_buf, n, want, sizeof(want));
    }
    n = encodeDisconnect(g_buf, sizeof(g_buf), kMqtt311, PacketType::Disconnect, d);
    {
        BYTES(0xE0, 0x00);
        expectBytes("DISCONNECT 3.1.1 drops the reason", g_buf, n, want, sizeof(want));
    }

    FixedHeader h;
    Disconnect dd;
    h.type = PacketType::Auth;
    EXPECT_EQ("AUTH in 3.1.1 rejected", decodeDisconnect(h, g_buf, 0, kMqtt311, dd), kProtocolError);
    h.type = PacketType::Disconnect;
    static const uint8_t b[] = {0x00};
    EXPECT_EQ("DISCONNECT 3.1.1 with a body rejected", decodeDisconnect(h, b, 1, kMqtt311, dd), kMalformedPacket);
}

static void testSmallBuffer()
{
    Publish p;
    p.topic = Bytes("a/b");
    p.payload = Bytes("hello");
    size_t full = encodePublish(g_buf, sizeof(g_buf), kMqtt311, p);
    EXPECT_EQ("encode into a buffer one byte short", encodePublish(g_buf, full - 1, kMqtt311, p), (size_t)0);
    EXPECT_EQ("encode into an exact buffer", encodePublish(g_buf, full, kMqtt311, p), full);
}

static void testReasonMapping()
{
    bool ok = true;
    for (uint8_t code = 0; code <= 5; ++code)
        if (connackCodeFromReason(reasonFromConnackCode(code)) != code)
            ok = false;
    EXPECT_TRUE("CONNACK codes map both ways", ok);
}

void setup()
{
    Serial.begin(115200);
    delay(5000);  // give the real ESP32 Serial-USB bridge time to settle
    testVarint();
    testUtf8();
    testFixedHeader();
    testConnect();
    testConnack();
    testPublish();
    testAcks();
    testSubscribe();
    testSubAck();
    testSmallPackets();
    testSmallBuffer();
    testReasonMapping();
    finishTests();
}

void loop()
{
    delay(1);
}
