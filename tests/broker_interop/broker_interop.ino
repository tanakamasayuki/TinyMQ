// TinyMQBroker with a TCP listener, driven by serial commands, for the
// interop tests with paho-mqtt and raw sockets (test_broker_interop.py).
//
// Commands (one per line):
//   SYS <s> ($SYS interval, 0: off)
//   LISTEN <tcp port> [<MQTT-SN udp port>]   (boards also listen on lwIP's raw API, port 1885)   SNRETRY <ms> <count>   MAXQOS <0-2>   EXPIRY <s>   CTIMEOUT <ms>
//   AUTH <0|1>  (user alice / password secret)          ACL <0|1>  (deny "secret/...")
//   LSUB <qos> <filter>   LUNSUB <filter>   LPUB <qos> <retain> <topic> <payload>
//   REBOOT (save the state, forget everything, load it)   STATS
// Settings commands answer OK <command>. Events printed:
//   LISTENING <port>   CLIENT <id> <1|0>   LOCAL <qos> <retain> <topic> <payload>
//   STATS clients=<n> sessions=<n> subs=<n> retained=<n> pool=<used>/<capacity> dropped=<n>

#include <TinyMQ.h>

#include "board_net.h"

WiFiServer server;
TinyMQTcpListener<WiFiServer> listener(server);
WiFiUDP udp;
TinyMQSnListener sn(udp);
#if defined(TINYMQ_HAS_LWIP) && defined(BOARD_NET_WIFI)
// the same broker on lwIP's raw API too (board only; the host core has no lwIP)
TinyMQLwipListener lwipListener(1885);
#endif
TinyMQBroker broker;

static char g_line[512];

// REBOOT: the state goes through RAM, as it would through a file
struct RamState : tinymq::StateSink, tinymq::StateSource {
    uint8_t data[8192];
    size_t len = 0, pos = 0;
    bool write(const uint8_t *d, size_t n) override
    {
        if (n > sizeof(data) - len)
            return false;
        memcpy(data + len, d, n);
        len += n;
        return true;
    }
    bool read(uint8_t *d, size_t n) override
    {
        if (n > len - pos)
            return false;
        memcpy(d, data + pos, n);
        pos += n;
        return true;
    }
};
static RamState g_state;
static unsigned g_retainedChanges;

static void onRetained(tinymq::Bytes, bool, void *)
{
    ++g_retainedChanges;
}
static size_t g_lineLen;

static void onLocal(const tinymq::Message &m, void *)
{
    Serial.print("LOCAL ");
    Serial.print(m.qos);
    Serial.print(' ');
    Serial.print(m.retain ? 1 : 0);
    Serial.print(' ');
    Serial.print(m.topic);
    Serial.print(' ');
    Serial.write(m.payload, m.payloadLen < 200 ? m.payloadLen : 200);
    Serial.println();
}

static void onClient(tinymq::Bytes id, bool connected, void *)
{
    Serial.print("CLIENT ");
    Serial.write(id.data, id.len);
    Serial.print(' ');
    Serial.println(connected ? 1 : 0);
}

static uint8_t onAuth(const tinymq::ConnectInfo &info, void *)
{
    if (!info.hasUsername || !info.username.equals("alice"))
        return tinymq::kNotAuthorized;
    if (!info.hasPassword || !info.password.equals("secret"))
        return tinymq::kBadUserNameOrPassword;
    return tinymq::kSuccess;
}

static bool onAcl(tinymq::Bytes, tinymq::Bytes, tinymq::Bytes topic, bool, void *)
{
    return !(topic.len >= 7 && memcmp(topic.data, "secret/", 7) == 0);
}

static char *nextWord(char **s)
{
    while (**s == ' ')
        ++*s;
    char *w = *s;
    while (**s && **s != ' ')
        ++*s;
    if (**s)
        *(*s)++ = 0;
    return w;
}

static void command(char *line)
{
    char *s = line;
    char *cmd = nextWord(&s);
    if (!strcmp(cmd, "LISTEN")) {
        uint16_t port = (uint16_t)atoi(nextWord(&s));
        uint16_t snPort = (uint16_t)atoi(nextWord(&s));
        server.begin(port);
        if (snPort)
            udp.begin(snPort);
        Serial.print("LISTENING ");
        Serial.print(port);
#if defined(TINYMQ_HAS_LWIP) && defined(BOARD_NET_WIFI)
        if (lwipListener.begin()) {
            broker.addListener(lwipListener);
            Serial.print(" lwip=1885");
        }
#endif
        Serial.println();
    } else if (!strcmp(cmd, "SNRETRY")) {
        uint32_t ms = (uint32_t)atol(nextWord(&s));
        sn.setRetry(ms, (uint8_t)atoi(nextWord(&s)));
    } else if (!strcmp(cmd, "MAXQOS")) {
        broker.setMaxQos((uint8_t)atoi(nextWord(&s)));
    } else if (!strcmp(cmd, "CTIMEOUT")) {
        broker.setConnectTimeout((uint32_t)atol(nextWord(&s)));
    } else if (!strcmp(cmd, "SYS")) {
        broker.setSysInterval((uint32_t)atol(nextWord(&s)));
    } else if (!strcmp(cmd, "EXPIRY")) {
        broker.setSessionExpiry((uint32_t)atol(nextWord(&s)));
    } else if (!strcmp(cmd, "AUTH")) {
        broker.onAuthenticate(atoi(nextWord(&s)) ? onAuth : nullptr);
    } else if (!strcmp(cmd, "ACL")) {
        broker.onAuthorize(atoi(nextWord(&s)) ? onAcl : nullptr);
    } else if (!strcmp(cmd, "LSUB")) {
        uint8_t qos = (uint8_t)atoi(nextWord(&s));
        bool ok = broker.subscribe(nextWord(&s), qos);  // may print LOCAL lines first
        Serial.println(ok ? "LSUB ok" : "LSUB fail");
    } else if (!strcmp(cmd, "LUNSUB")) {
        Serial.println(broker.unsubscribe(nextWord(&s)) ? "LUNSUB ok" : "LUNSUB fail");
    } else if (!strcmp(cmd, "LPUB")) {
        uint8_t qos = (uint8_t)atoi(nextWord(&s));
        bool retain = atoi(nextWord(&s)) != 0;
        char *topic = nextWord(&s);
        bool ok = broker.publish(topic, s, qos, retain);
        Serial.println(ok ? "LPUB ok" : "LPUB fail");
    } else if (!strcmp(cmd, "REBOOT")) {
        // save, forget everything (clients are cut off), load
        g_state.len = g_state.pos = 0;
        bool saved = broker.saveState(g_state);
        broker.end();
        bool loaded = saved && broker.loadState(g_state);
        Serial.print(loaded ? "REBOOT ok " : "REBOOT fail ");
        Serial.print((unsigned)g_state.len);
        Serial.print(' ');
        Serial.println(g_retainedChanges);
    } else if (!strcmp(cmd, "STATS")) {
        Serial.print("STATS clients=");
        Serial.print((unsigned)broker.clients());
        Serial.print(" sessions=");
        Serial.print((unsigned)broker.sessions());
        Serial.print(" subs=");
        Serial.print((unsigned)broker.subscriptions());
        Serial.print(" retained=");
        Serial.print((unsigned)broker.retained());
        Serial.print(" pool=");
        Serial.print((unsigned)broker.poolUsed());
        Serial.print('/');
        Serial.print((unsigned)broker.poolCapacity());
        Serial.print(" dropped=");
        Serial.print((unsigned long)broker.droppedMessages());
        Serial.print(" sn=");
        Serial.print((unsigned)sn.clients());
        Serial.print(" snregs=");
        Serial.println((unsigned)sn.gateway().registrations());
    } else {
        Serial.print("ERR unknown ");
        Serial.println(cmd);
        return;
    }
    if (!strcmp(cmd, "MAXQOS") || !strcmp(cmd, "SYS") || !strcmp(cmd, "SNRETRY") || !strcmp(cmd, "CTIMEOUT") || !strcmp(cmd, "EXPIRY") || !strcmp(cmd, "AUTH") || !strcmp(cmd, "ACL")) {
        Serial.print("OK ");
        Serial.println(cmd);
    }
}

void setup()
{
    Serial.begin(115200);
    if (!networkReady())
        return;
    broker.begin();
    broker.addListener(listener);
    broker.addListener(sn);
    sn.addPredefinedTopic(1, "pre/one");
    sn.addPredefinedTopic(2, "pre/two");
    broker.onMessage(onLocal);
    broker.onClient(onClient);
    broker.onRetainedChange(onRetained);
}

void loop()
{
    while (Serial.available()) {
        int c = Serial.read();
        if (c == '\r')
            continue;
        if (c == '\n') {
            g_line[g_lineLen] = 0;
            if (g_lineLen)
                command(g_line);
            g_lineLen = 0;
        } else if (g_lineLen + 1 < sizeof(g_line)) {
            g_line[g_lineLen++] = (char)c;
        }
    }
    broker.update();
    delay(1);
}
