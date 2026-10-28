// TinyMQClient driven by serial commands, for the interop tests against
// mosquitto and a scripted fake broker (test_client_interop.py).
//
// Commands (one per line):
//   SERVER <ip> <port>       ID <client id>        CLEAN <0|1>
//   KEEPALIVE <s>            PROTO <3|4>            AUTH <user> <pass>   NOAUTH
//   WILL <qos> <retain> <topic> <payload>          NOWILL
//   CONNECT                  DISCONNECT             DROP (close the socket without DISCONNECT)
//   SUB <qos> <filter>       UNSUB <filter>
//   PUB <qos> <retain> <topic> <payload>           STATUS
// Events printed:
//   CONNECT ok|fail <reason>   CONNACK <reason> <session present>
//   SUB <id>  UNSUB <id>  PUB <id>  SUBACK <id> <codes...>  UNSUBACK <id>
//   PUBDONE <id> <reason>  MSG <qos> <retain> <dup> <topic> <len> <payload>
//   CLOSED <reason>  STATUS connected=<0|1> inflight=<n> dropped=<n>

#include <TinyMQ.h>

#include "board_net.h"

WiFiClient net;
TinyMQClient mqtt(net);

static char g_host[32];
static char g_id[64];
static char g_user[32];
static char g_pass[32];
static char g_willTopic[64];
static char g_willPayload[64];
static char g_line[512];
static size_t g_lineLen;

static void printHex2(uint8_t v)
{
    if (v < 16)
        Serial.print('0');
    Serial.print(v, HEX);
}

static void onMessage(const tinymq::Message &m, void *)
{
    Serial.print("MSG ");
    Serial.print(m.qos);
    Serial.print(' ');
    Serial.print(m.retain ? 1 : 0);
    Serial.print(' ');
    Serial.print(m.dup ? 1 : 0);
    Serial.print(' ');
    Serial.print(m.topic);
    Serial.print(' ');
    Serial.print((unsigned)m.payloadLen);
    Serial.print(' ');
    size_t n = m.payloadLen < 200 ? m.payloadLen : 200;
    Serial.write(m.payload, n);
    Serial.println();
}

static void onConnack(uint8_t reason, bool sessionPresent, void *)
{
    Serial.print("CONNACK ");
    printHex2(reason);
    Serial.print(' ');
    Serial.println(sessionPresent ? 1 : 0);
}

static void onSuback(uint16_t id, bool unsubscribe, const uint8_t *codes, size_t count, void *)
{
    Serial.print(unsubscribe ? "UNSUBACK " : "SUBACK ");
    Serial.print(id);
    for (size_t i = 0; i < count; ++i) {
        Serial.print(' ');
        printHex2(codes[i]);
    }
    Serial.println();
}

static void onPublished(uint16_t id, uint8_t reason, void *)
{
    Serial.print("PUBDONE ");
    Serial.print(id);
    Serial.print(' ');
    printHex2(reason);
    Serial.println();
}

static void onClosed(uint8_t reason, void *)
{
    Serial.print("CLOSED ");
    printHex2(reason);
    Serial.println();
}

// Splits off the next space-separated word of *s
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

static void copyTo(char *dst, size_t cap, const char *src)
{
    strncpy(dst, src, cap - 1);
    dst[cap - 1] = 0;
}

static void command(char *line)
{
    char *s = line;
    char *cmd = nextWord(&s);
    if (!strcmp(cmd, "SERVER")) {
        copyTo(g_host, sizeof(g_host), nextWord(&s));
        IPAddress ip;
        ip.fromString(g_host);
        mqtt.setServer(ip, (uint16_t)atoi(nextWord(&s)));
    } else if (!strcmp(cmd, "ID")) {
        copyTo(g_id, sizeof(g_id), nextWord(&s));
        mqtt.setClientId(g_id);
    } else if (!strcmp(cmd, "CLEAN")) {
        mqtt.setCleanSession(atoi(nextWord(&s)) != 0);
    } else if (!strcmp(cmd, "KEEPALIVE")) {
        mqtt.setKeepAlive((uint16_t)atoi(nextWord(&s)));
    } else if (!strcmp(cmd, "PROTO")) {
        mqtt.setProtocolVersion((uint8_t)atoi(nextWord(&s)));
    } else if (!strcmp(cmd, "AUTH")) {
        copyTo(g_user, sizeof(g_user), nextWord(&s));
        copyTo(g_pass, sizeof(g_pass), nextWord(&s));
        mqtt.setCredentials(g_user, g_pass);
    } else if (!strcmp(cmd, "NOAUTH")) {
        mqtt.setCredentials(nullptr, nullptr);
    } else if (!strcmp(cmd, "WILL")) {
        uint8_t qos = (uint8_t)atoi(nextWord(&s));
        bool retain = atoi(nextWord(&s)) != 0;
        copyTo(g_willTopic, sizeof(g_willTopic), nextWord(&s));
        copyTo(g_willPayload, sizeof(g_willPayload), s);
        mqtt.setWill(g_willTopic, g_willPayload, qos, retain);
    } else if (!strcmp(cmd, "NOWILL")) {
        mqtt.clearWill();
    } else if (!strcmp(cmd, "CONNECT")) {
        bool ok = mqtt.connect();
        Serial.print(ok ? "CONNECT ok " : "CONNECT fail ");
        printHex2(mqtt.lastReason());
        Serial.println();
    } else if (!strcmp(cmd, "DISCONNECT")) {
        mqtt.disconnect();
    } else if (!strcmp(cmd, "DROP")) {
        net.stop();
    } else if (!strcmp(cmd, "SUB")) {
        uint8_t qos = (uint8_t)atoi(nextWord(&s));
        Serial.print("SUB ");
        Serial.println(mqtt.subscribe(nextWord(&s), qos));
    } else if (!strcmp(cmd, "UNSUB")) {
        Serial.print("UNSUB ");
        Serial.println(mqtt.unsubscribe(nextWord(&s)));
    } else if (!strcmp(cmd, "PUB")) {
        uint8_t qos = (uint8_t)atoi(nextWord(&s));
        bool retain = atoi(nextWord(&s)) != 0;
        char *topic = nextWord(&s);
        Serial.print("PUB ");
        Serial.println(mqtt.publish(topic, s, qos, retain));
    } else if (!strcmp(cmd, "STATUS")) {
        Serial.print("STATUS connected=");
        Serial.print(mqtt.connected() ? 1 : 0);
        Serial.print(" inflight=");
        Serial.print((unsigned)mqtt.inflight());
        Serial.print(" dropped=");
        Serial.println((unsigned long)mqtt.droppedMessages());
    } else {
        Serial.print("ERR unknown ");
        Serial.println(cmd);
    }
}

void setup()
{
    Serial.begin(115200);
    if (!networkReady())
        return;
    mqtt.onMessage(onMessage);
    mqtt.onConnect(onConnack);
    mqtt.onSubscribe(onSuback);
    mqtt.onPublished(onPublished);
    mqtt.onDisconnect(onClosed);
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
    mqtt.update();
    delay(1);
}
