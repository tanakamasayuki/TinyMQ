// Topic unit tests: name and filter validity, matching (the examples of
// MQTT 3.1.1 §4.7 and 5.0 §4.7), `$` topics and shared subscriptions.

#include <TinyMQ.h>
#include "expect.h"

using namespace tinymq;

struct MatchCase {
    const char *filter;
    const char *topic;
    bool match;
};

static const MatchCase kMatches[] = {
    {"sport/tennis/player1/#", "sport/tennis/player1", true},
    {"sport/tennis/player1/#", "sport/tennis/player1/ranking", true},
    {"sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon", true},
    {"sport/#", "sport", true},
    {"#", "sport/tennis", true},
    {"#", "/", true},
    {"sport/tennis/+", "sport/tennis/player1", true},
    {"sport/tennis/+", "sport/tennis/player2", true},
    {"sport/tennis/+", "sport/tennis/player1/ranking", false},
    {"sport/+", "sport", false},
    {"sport/+", "sport/", true},
    {"+/+", "/finance", true},
    {"/+", "/finance", true},
    {"+", "/finance", false},
    {"+/tennis/#", "sport/tennis/player1", true},
    {"a/b", "a/b", true},
    {"a/b", "a/b/", false},
    {"a/b", "a/bc", false},
    {"a/bc", "a/b", false},
    {"a/b", "A/b", false},
    {"a//b", "a//b", true},
    {"a/+/b", "a//b", true},
    {"a/#", "a", true},
    {"a/#", "ab", false},
    {"+/#", "a", true},
    {"#", "$SYS/broker", false},
    {"+/broker", "$SYS/broker", false},
    {"$SYS/#", "$SYS/broker", true},
    {"$SYS/+", "$SYS/broker", true},
    {"a/+/+/d", "a/b/c/d", true},
    {"a/+/+/d", "a/b/c", false},
    {"a/+", "a/b/c", false},
};

static void testMatching()
{
    for (const MatchCase &c : kMatches) {
        char name[96];
        snprintf(name, sizeof(name), "'%s' %s '%s'", c.filter, c.match ? "matches" : "does not match", c.topic);
        EXPECT_EQ(name, topicMatches(Bytes(c.filter), Bytes(c.topic)), c.match);
    }
}

static void testValidity()
{
    static const char *goodFilters[] = {"#", "+", "a/#", "a/+/b", "+/+", "/", "//", "a//#", "+/#", "$SYS/#"};
    static const char *badFilters[] = {"", "a#", "a/#/b", "#/a", "a+", "+a", "a/b+", "a/+b/c", "##"};
    bool ok = true;
    for (const char *f : goodFilters)
        if (!validTopicFilter(Bytes(f))) {
            ok = false;
            Serial.print("  should be valid: ");
            Serial.println(f);
        }
    EXPECT_TRUE("valid filters accepted", ok);
    ok = true;
    for (const char *f : badFilters)
        if (validTopicFilter(Bytes(f))) {
            ok = false;
            Serial.print("  should be invalid: ");
            Serial.println(f);
        }
    EXPECT_TRUE("invalid filters rejected", ok);

    EXPECT_TRUE("topic name plain", validTopicName(Bytes("a/b")));
    EXPECT_TRUE("topic name of slashes", validTopicName(Bytes("/")));
    EXPECT_TRUE("topic name empty rejected", !validTopicName(Bytes("")));
    EXPECT_TRUE("topic name with + rejected", !validTopicName(Bytes("a/+")));
    EXPECT_TRUE("topic name with # rejected", !validTopicName(Bytes("a/#")));
}

static void testShared()
{
    Bytes group, inner;
    EXPECT_TRUE("not shared", !parseSharedFilter(Bytes("a/b"), group, inner));
    EXPECT_TRUE("not shared without the slash", !parseSharedFilter(Bytes("$share"), group, inner));
    bool ok = parseSharedFilter(Bytes("$share/g1/a/+"), group, inner);
    EXPECT_TRUE("shared filter split", ok && group.equals("g1") && inner.equals("a/+"));
    ok = parseSharedFilter(Bytes("$share//a"), group, inner);
    EXPECT_TRUE("shared with an empty group is malformed", ok && group.len == 0);
    ok = parseSharedFilter(Bytes("$share/g1"), group, inner);
    EXPECT_TRUE("shared without a filter is malformed", ok && group.len == 0);
    ok = parseSharedFilter(Bytes("$share/g1/"), group, inner);
    EXPECT_TRUE("shared with an empty filter is malformed", ok && group.len == 0);
    ok = parseSharedFilter(Bytes("$share/g+/a"), group, inner);
    EXPECT_TRUE("shared with a wildcard group is malformed", ok && group.len == 0);
}

void setup()
{
    Serial.begin(115200);
    delay(5000);  // give the real ESP32 Serial-USB bridge time to settle
    testMatching();
    testValidity();
    testShared();
    finishTests();
}

void loop()
{
    delay(1);
}
