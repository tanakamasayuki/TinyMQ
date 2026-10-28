// MQTT 5.0 property unit tests: building, reading back, the checks of
// mq_props.h (types, ranges, scopes, duplicates) and copying with skips.

#include <TinyMQ.h>
#include "expect.h"

using namespace tinymq;

static uint8_t g_buf[256];
static uint8_t g_out[256];

static Bytes sample()
{
    PropWriter w(g_buf, sizeof(g_buf));
    w.byte(kPayloadFormatIndicator, 1);
    w.u32(kMessageExpiryInterval, 60);
    w.string(kContentType, Bytes("text/plain"));
    w.pair(kUserProperty, Bytes("a"), Bytes("1"));
    w.pair(kUserProperty, Bytes("b"), Bytes("2"));
    w.varint(kSubscriptionIdentifier, 300);
    w.varint(kSubscriptionIdentifier, 5);
    return w.result();
}

static void testBuildRead()
{
    Bytes b = sample();
    static const uint8_t want[] = {0x01, 0x01, 0x02, 0x00, 0x00, 0x00, 0x3C, 0x03, 0x00, 0x0A, 't', 'e', 'x', 't', '/', 'p', 'l', 'a', 'i', 'n',
                                   0x26, 0x00, 0x01, 'a', 0x00, 0x01, '1', 0x26, 0x00, 0x01, 'b', 0x00, 0x01, '2', 0x0B, 0xAC, 0x02, 0x0B, 0x05};
    expectBytes("built block", b.data, b.len, want, sizeof(want));
    PropReader r(b);
    Property p;
    int n = 0;
    bool ok = true;
    while (r.next(p)) {
        switch (n++) {
        case 0: ok &= p.id == kPayloadFormatIndicator && p.value == 1; break;
        case 1: ok &= p.id == kMessageExpiryInterval && p.value == 60; break;
        case 2: ok &= p.id == kContentType && p.data.equals("text/plain"); break;
        case 3: ok &= p.id == kUserProperty && p.data.equals("a") && p.data2.equals("1"); break;
        case 4: ok &= p.id == kUserProperty && p.data.equals("b") && p.data2.equals("2"); break;
        case 5: ok &= p.id == kSubscriptionIdentifier && p.value == 300; break;
        case 6: ok &= p.id == kSubscriptionIdentifier && p.value == 5; break;
        }
    }
    EXPECT_TRUE("read back every property", ok && n == 7 && r.ok());
    Property f;
    EXPECT_TRUE("find the content type", findProp(b, kContentType, f) && f.data.equals("text/plain"));
    EXPECT_TRUE("find a missing one", !findProp(b, kResponseTopic, f));
}

static void testChecks()
{
    Bytes b = sample();
    EXPECT_EQ("PUBLISH properties pass", checkProps(b, kInPublish), kSuccess);
    EXPECT_EQ("two subscription ids outside PUBLISH", checkProps(b, kInWill | kInSubscribe), kProtocolError);
    EXPECT_EQ("empty block passes", checkProps(Bytes(), kInConnect), kSuccess);

    struct Case {
        const char *name;
        uint8_t data[12];
        uint8_t len;
        uint16_t scope;
        uint8_t want;
    };
    static const Case cases[] = {
        {"content type twice", {0x03, 0x00, 0x01, 'a', 0x03, 0x00, 0x01, 'b'}, 8, kInPublish, kProtocolError},
        {"session expiry in PUBLISH", {0x11, 0, 0, 0, 1}, 5, kInPublish, kProtocolError},
        {"session expiry in CONNECT", {0x11, 0, 0, 0, 1}, 5, kInConnect, kSuccess},
        {"payload format 2", {0x01, 0x02}, 2, kInPublish, kProtocolError},
        {"topic alias 0", {0x23, 0x00, 0x00}, 3, kInPublish, kProtocolError},
        {"receive maximum 0", {0x21, 0x00, 0x00}, 3, kInConnect, kProtocolError},
        {"subscription id 0", {0x0B, 0x00}, 2, kInSubscribe, kProtocolError},
        {"unknown id", {0x05, 0x00}, 2, kInPublish, kMalformedPacket},
        {"truncated u32", {0x02, 0x00, 0x00}, 3, kInPublish, kMalformedPacket},
        {"truncated string", {0x03, 0x00, 0x05, 'a'}, 4, kInPublish, kMalformedPacket},
        {"bad UTF-8", {0x03, 0x00, 0x02, 0xC0, 0x80}, 5, kInPublish, kMalformedPacket},
        {"will delay in a will", {0x18, 0, 0, 0, 5}, 5, kInWill, kSuccess},
        {"will delay in PUBLISH", {0x18, 0, 0, 0, 5}, 5, kInPublish, kProtocolError},
        {"reason string in PUBACK", {0x1F, 0x00, 0x02, 'o', 'k'}, 5, kInAck, kSuccess},
        {"user property anywhere", {0x26, 0x00, 0x01, 'k', 0x00, 0x00}, 6, kInAuth, kSuccess},
    };
    for (const Case &c : cases)
        EXPECT_EQ(c.name, checkProps(Bytes(c.data, c.len), c.scope), c.want);
}

static void testCopy()
{
    Bytes b = sample();
    PropWriter w(g_out, sizeof(g_out));
    static const uint8_t skip[] = {kMessageExpiryInterval, kSubscriptionIdentifier};
    w.copy(b, skip, sizeof(skip));
    Bytes c = w.result();
    Property p;
    EXPECT_TRUE("copy keeps the others", findProp(c, kContentType, p) && findProp(c, kUserProperty, p) && findProp(c, kPayloadFormatIndicator, p));
    EXPECT_TRUE("copy leaves out the skipped", !findProp(c, kMessageExpiryInterval, p) && !findProp(c, kSubscriptionIdentifier, p));
    EXPECT_EQ("copied length", c.len, b.len - 5u - 3u - 2u);
    PropWriter all(g_out, sizeof(g_out));
    all.copy(b);
    expectBytes("copy without skips is identical", all.result().data, all.result().len, b.data, b.len);
    PropWriter small(g_out, 4);
    small.copy(b);
    EXPECT_TRUE("copy into a short buffer fails", !small.ok());
}

void setup()
{
    Serial.begin(115200);
    delay(5000);  // give the real ESP32 Serial-USB bridge time to settle
    testBuildRead();
    testChecks();
    testCopy();
    finishTests();
}

void loop()
{
    delay(1);
}
