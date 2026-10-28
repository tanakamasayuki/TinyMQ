// MQTT-SN 1.2 codec unit tests: the flags byte, the short and long length
// forms, and every message the gateway and the client use, against
// hand-built bytes from the specification.

#include <TinyMQ.h>
#include "expect.h"

using namespace tinymq::sn;
using tinymq::Bytes;

static uint8_t g_buf[512];

#define BYTES(...) \
    static const uint8_t want[] = {__VA_ARGS__}

static bool split(const uint8_t *d, size_t len, uint8_t wantType, const uint8_t *&body, size_t &bodyLen)
{
    uint8_t type;
    return parseHeader(d, len, type, body, bodyLen) && type == wantType;
}

static void testFlags()
{
    Flags f;
    f.dup = true;
    f.qos = 2;
    f.retain = true;
    f.topicType = kTopicShort;
    EXPECT_EQ("flags DUP QoS 2 retain short", f.encode(), (uint8_t)0xD2);
    f = Flags();
    f.qos = -1;
    f.topicType = kTopicPredefined;
    EXPECT_EQ("flags QoS -1 predefined", f.encode(), (uint8_t)0x61);
    Flags d = Flags::decode(0x6C);  // QoS -1, will, clean
    EXPECT_TRUE("flags decode", d.qos == -1 && d.will && d.clean && !d.retain && !d.dup && d.topicType == 0);
}

static void testHeader()
{
    uint8_t type;
    const uint8_t *body;
    size_t len;
    static const uint8_t ping[] = {0x02, 0x17};
    EXPECT_TRUE("header short form", parseHeader(ping, 2, type, body, len) && type == kPingresp && len == 0);
    static const uint8_t mismatch[] = {0x03, 0x17};
    EXPECT_TRUE("header length mismatch rejected", !parseHeader(mismatch, 2, type, body, len));
    static const uint8_t tooShort[] = {0x01, 0x17};
    EXPECT_TRUE("header length below 2 rejected", !parseHeader(tooShort, 2, type, body, len));
    static const uint8_t longSmall[] = {0x01, 0x00, 0x05, 0x17, 0x00};
    EXPECT_TRUE("long form under 256 rejected", !parseHeader(longSmall, 5, type, body, len));

    static uint8_t data[300];
    memset(data, 'x', sizeof(data));
    Publish p;
    p.flags.qos = 0;
    p.topicId = 1;
    p.data = Bytes(data, sizeof(data));
    size_t n = encodePublish(g_buf, sizeof(g_buf), p);
    EXPECT_TRUE("long form used from 256 octets", n == 309 && g_buf[0] == 0x01 && g_buf[1] == 0x01 && g_buf[2] == 0x35 && g_buf[3] == kPublish);
    Publish d;
    EXPECT_TRUE("long form decodes", split(g_buf, n, kPublish, body, len) && decodePublish(body, len, d) && d.data.len == 300);
    // the boundary: 254 + 2 = 256 needs the long form, 253 + 2 = 255 does not
    static uint8_t raw[260];
    EXPECT_EQ("255 octets in the short form", encodeRaw(g_buf, sizeof(g_buf), kWillMsg, Bytes(raw, 253)), (size_t)255);
    EXPECT_EQ("256 octets in the long form", encodeRaw(g_buf, sizeof(g_buf), kWillMsg, Bytes(raw, 254)), (size_t)258);
}

static void testConnect()
{
    Connect c;
    c.flags.clean = true;
    c.duration = 60;
    c.clientId = Bytes("abc");
    size_t n = encodeConnect(g_buf, sizeof(g_buf), c);
    {
        BYTES(0x09, 0x04, 0x04, 0x01, 0x00, 0x3C, 'a', 'b', 'c');
        expectBytes("CONNECT", g_buf, n, want, sizeof(want));
    }
    const uint8_t *body;
    size_t len;
    Connect d;
    EXPECT_TRUE("CONNECT decodes", split(g_buf, n, kConnect, body, len) && decodeConnect(body, len, d) && d.flags.clean && d.duration == 60 && d.clientId.equals("abc") && d.protocolId == 1);
    c.clientId = Bytes("abcdefghijklmnopqrstuvwx");  // 24
    n = encodeConnect(g_buf, sizeof(g_buf), c);
    EXPECT_TRUE("CONNECT client id over 23 rejected", split(g_buf, n, kConnect, body, len) && !decodeConnect(body, len, d));
    c.clientId = Bytes();
    n = encodeConnect(g_buf, sizeof(g_buf), c);
    EXPECT_TRUE("CONNECT empty client id rejected", split(g_buf, n, kConnect, body, len) && !decodeConnect(body, len, d));

    n = encodeConnack(g_buf, sizeof(g_buf), kAccepted);
    {
        BYTES(0x03, 0x05, 0x00);
        expectBytes("CONNACK", g_buf, n, want, sizeof(want));
    }
}

static void testWill()
{
    WillTopic t;
    t.present = true;
    t.flags.qos = 1;
    t.flags.retain = true;
    t.topic = Bytes("w/1");
    size_t n = encodeWillTopic(g_buf, sizeof(g_buf), kWillTopic, t);
    {
        BYTES(0x06, 0x07, 0x30, 'w', '/', '1');
        expectBytes("WILLTOPIC", g_buf, n, want, sizeof(want));
    }
    const uint8_t *body;
    size_t len;
    WillTopic d;
    EXPECT_TRUE("WILLTOPIC decodes", split(g_buf, n, kWillTopic, body, len) && decodeWillTopic(body, len, d) && d.present && d.flags.qos == 1 && d.flags.retain && d.topic.equals("w/1"));
    WillTopic empty;
    n = encodeWillTopic(g_buf, sizeof(g_buf), kWillTopicUpd, empty);
    {
        BYTES(0x02, 0x1A);
        expectBytes("empty WILLTOPICUPD clears the will", g_buf, n, want, sizeof(want));
    }
    EXPECT_TRUE("empty WILLTOPICUPD decodes", split(g_buf, n, kWillTopicUpd, body, len) && decodeWillTopic(body, len, d) && !d.present);
    n = encodeEmpty(g_buf, sizeof(g_buf), kWillTopicReq);
    {
        BYTES(0x02, 0x06);
        expectBytes("WILLTOPICREQ", g_buf, n, want, sizeof(want));
    }
}

static void testPublish()
{
    Publish p;
    p.flags.qos = 1;
    p.topicId = 1;
    p.msgId = 2;
    p.data = Bytes("hi");
    size_t n = encodePublish(g_buf, sizeof(g_buf), p);
    {
        BYTES(0x09, 0x0C, 0x20, 0x00, 0x01, 0x00, 0x02, 'h', 'i');
        expectBytes("PUBLISH QoS 1", g_buf, n, want, sizeof(want));
    }
    p = Publish();
    p.flags.qos = -1;
    p.flags.topicType = kTopicShort;
    p.topicId = ('a' << 8) | 'b';
    p.data = Bytes("x");
    n = encodePublish(g_buf, sizeof(g_buf), p);
    {
        BYTES(0x08, 0x0C, 0x62, 'a', 'b', 0x00, 0x00, 'x');
        expectBytes("PUBLISH QoS -1 short topic", g_buf, n, want, sizeof(want));
    }
    const uint8_t *body;
    size_t len;
    Publish d;
    EXPECT_TRUE("PUBLISH decodes", split(g_buf, n, kPublish, body, len) && decodePublish(body, len, d) && d.flags.qos == -1 && d.flags.topicType == kTopicShort && d.topicId == 0x6162 && d.data.equals("x"));
    static const uint8_t reserved[] = {0x07, 0x0C, 0x03, 0x00, 0x01, 0x00, 0x00};
    EXPECT_TRUE("PUBLISH topic type 3 rejected", split(reserved, sizeof(reserved), kPublish, body, len) && !decodePublish(body, len, d));
    static const uint8_t truncated[] = {0x06, 0x0C, 0x00, 0x00, 0x01, 0x00};
    EXPECT_TRUE("PUBLISH truncated rejected", split(truncated, sizeof(truncated), kPublish, body, len) && !decodePublish(body, len, d));

    TopicAck a;
    a.topicId = 1;
    a.msgId = 2;
    a.rc = kInvalidTopicId;
    n = encodeTopicAck(g_buf, sizeof(g_buf), kPuback, a);
    {
        BYTES(0x07, 0x0D, 0x00, 0x01, 0x00, 0x02, 0x02);
        expectBytes("PUBACK invalid topic id", g_buf, n, want, sizeof(want));
    }
    n = encodeMsgId(g_buf, sizeof(g_buf), kPubrel, 0x1234);
    {
        BYTES(0x04, 0x10, 0x12, 0x34);
        expectBytes("PUBREL", g_buf, n, want, sizeof(want));
    }
    uint16_t id;
    EXPECT_TRUE("PUBREL decodes", split(g_buf, n, kPubrel, body, len) && decodeMsgId(body, len, id) && id == 0x1234);
}

static void testRegister()
{
    Register r;
    r.topicId = 0;
    r.msgId = 5;
    r.topicName = Bytes("a/b");
    size_t n = encodeRegister(g_buf, sizeof(g_buf), r);
    {
        BYTES(0x09, 0x0A, 0x00, 0x00, 0x00, 0x05, 'a', '/', 'b');
        expectBytes("REGISTER", g_buf, n, want, sizeof(want));
    }
    const uint8_t *body;
    size_t len;
    Register d;
    EXPECT_TRUE("REGISTER decodes", split(g_buf, n, kRegister, body, len) && decodeRegister(body, len, d) && d.msgId == 5 && d.topicName.equals("a/b"));
    TopicAck a;
    a.topicId = 7;
    a.msgId = 5;
    n = encodeTopicAck(g_buf, sizeof(g_buf), kRegack, a);
    TopicAck ad;
    EXPECT_TRUE("REGACK decodes", split(g_buf, n, kRegack, body, len) && decodeTopicAck(body, len, ad) && ad.topicId == 7 && ad.msgId == 5 && ad.rc == 0);
}

static void testSubscribe()
{
    Subscribe s;
    s.flags.qos = 1;
    s.msgId = 3;
    s.topicName = Bytes("s/#");
    size_t n = encodeSubscribe(g_buf, sizeof(g_buf), kSubscribe, s);
    {
        BYTES(0x08, 0x12, 0x20, 0x00, 0x03, 's', '/', '#');
        expectBytes("SUBSCRIBE by name", g_buf, n, want, sizeof(want));
    }
    const uint8_t *body;
    size_t len;
    Subscribe d;
    EXPECT_TRUE("SUBSCRIBE by name decodes", split(g_buf, n, kSubscribe, body, len) && decodeSubscribe(body, len, d) && d.flags.qos == 1 && d.msgId == 3 && d.topicName.equals("s/#"));
    s.flags.topicType = kTopicPredefined;
    s.topicId = 0x0102;
    n = encodeSubscribe(g_buf, sizeof(g_buf), kUnsubscribe, s);
    {
        BYTES(0x07, 0x14, 0x21, 0x00, 0x03, 0x01, 0x02);
        expectBytes("UNSUBSCRIBE predefined", g_buf, n, want, sizeof(want));
    }
    EXPECT_TRUE("UNSUBSCRIBE decodes", split(g_buf, n, kUnsubscribe, body, len) && decodeSubscribe(body, len, d) && d.flags.topicType == kTopicPredefined && d.topicId == 0x0102);

    Suback a;
    a.flags.qos = 1;
    a.topicId = 9;
    a.msgId = 3;
    n = encodeSuback(g_buf, sizeof(g_buf), a);
    {
        BYTES(0x08, 0x13, 0x20, 0x00, 0x09, 0x00, 0x03, 0x00);
        expectBytes("SUBACK", g_buf, n, want, sizeof(want));
    }
    Suback ad;
    EXPECT_TRUE("SUBACK decodes", split(g_buf, n, kSuback, body, len) && decodeSuback(body, len, ad) && ad.flags.qos == 1 && ad.topicId == 9 && ad.msgId == 3);
}

static void testOthers()
{
    size_t n = encodeDisconnect(g_buf, sizeof(g_buf), false, 0);
    {
        BYTES(0x02, 0x18);
        expectBytes("DISCONNECT", g_buf, n, want, sizeof(want));
    }
    n = encodeDisconnect(g_buf, sizeof(g_buf), true, 300);
    {
        BYTES(0x04, 0x18, 0x01, 0x2C);
        expectBytes("DISCONNECT with sleep duration", g_buf, n, want, sizeof(want));
    }
    n = encodePingreq(g_buf, sizeof(g_buf), Bytes("abc"));
    {
        BYTES(0x05, 0x16, 'a', 'b', 'c');
        expectBytes("PINGREQ with client id", g_buf, n, want, sizeof(want));
    }
    n = encodeAdvertise(g_buf, sizeof(g_buf), 1, 900);
    {
        BYTES(0x05, 0x00, 0x01, 0x03, 0x84);
        expectBytes("ADVERTISE", g_buf, n, want, sizeof(want));
    }
    n = encodeSearchGw(g_buf, sizeof(g_buf), 1);
    {
        BYTES(0x03, 0x01, 0x01);
        expectBytes("SEARCHGW", g_buf, n, want, sizeof(want));
    }
    n = encodeGwInfo(g_buf, sizeof(g_buf), 7);
    {
        BYTES(0x03, 0x02, 0x07);
        expectBytes("GWINFO from a gateway", g_buf, n, want, sizeof(want));
    }
    EXPECT_EQ("encode into a short buffer fails", encodeGwInfo(g_buf, 2, 7), (size_t)0);
}

void setup()
{
    Serial.begin(115200);
    delay(5000);  // give the real ESP32 Serial-USB bridge time to settle
    testFlags();
    testHeader();
    testConnect();
    testWill();
    testPublish();
    testRegister();
    testSubscribe();
    testOthers();
    finishTests();
}

void loop()
{
    delay(1);
}
