// TinyMQBroker with a TCP listener, driven by serial commands, for the
// interop tests with paho-mqtt and raw sockets (test_broker_interop.py).
//
// Commands (one per line):
//   LISTEN <port>   MAXQOS <0-2>   EXPIRY <s>   CTIMEOUT <ms>
//   AUTH <0|1>  (user alice / password secret)          ACL <0|1>  (deny "secret/...")
//   LSUB <qos> <filter>   LUNSUB <filter>   LPUB <qos> <retain> <topic> <payload>
//   STATS
// Settings commands answer OK <command>. Events printed:
//   LISTENING <port>   CLIENT <id> <1|0>   LOCAL <qos> <retain> <topic> <payload>
//   STATS clients=<n> sessions=<n> subs=<n> retained=<n> pool=<used>/<capacity> dropped=<n>

#include <WiFi.h>
#include <TinyMQ.h>

WiFiServer server;
TinyMQTcpListener<WiFiServer> listener(server);
TinyMQBroker broker;

static char g_line[512];
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
        server.begin(port);
        Serial.print("LISTENING ");
        Serial.println(port);
    } else if (!strcmp(cmd, "MAXQOS")) {
        broker.setMaxQos((uint8_t)atoi(nextWord(&s)));
    } else if (!strcmp(cmd, "CTIMEOUT")) {
        broker.setConnectTimeout((uint32_t)atol(nextWord(&s)));
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
        Serial.println((unsigned long)broker.droppedMessages());
    } else {
        Serial.print("ERR unknown ");
        Serial.println(cmd);
        return;
    }
    if (!strcmp(cmd, "MAXQOS") || !strcmp(cmd, "CTIMEOUT") || !strcmp(cmd, "EXPIRY") || !strcmp(cmd, "AUTH") || !strcmp(cmd, "ACL")) {
        Serial.print("OK ");
        Serial.println(cmd);
    }
}

void setup()
{
    Serial.begin(115200);
    delay(2000);
    broker.begin();
    broker.addListener(listener);
    broker.onMessage(onLocal);
    broker.onClient(onClient);
    Serial.println("READY");
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
