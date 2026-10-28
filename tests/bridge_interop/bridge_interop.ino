// TinyMQBroker with two bridges to an upstream broker (mosquitto on the PC),
// driven by serial commands, for test_bridge_interop.py.
//
// Commands (one per line):
//   BROKER <port>                         the local broker listens
//   BRIDGE <ip> <port> <version>          bridge A: out/# -> edge1/out/#, edge1/cmd/# -> cmd/#,
//                                         sync/# both ways, ret/# -> edge1/ret/#,
//                                         sensors/+/temp -> edge1/s/+/temp
//   BRIDGE2 <ip> <port>                   bridge B (3.1.1): o2/# -> edge2/o2/#, edge2/c2/# -> c2/#
//   LPUB <qos> <retain> <topic> <payload> publish from the sketch
//   STATS                                 BSTATS a=<connected> out=<n> in=<n> dropped=<n> queued=<n> b=<connected> out=<n> in=<n>

#include <TinyMQ.h>

#include "board_net.h"

WiFiServer server;
TinyMQTcpListener<WiFiServer> listener(server);
TinyMQBroker broker;

WiFiClient upstreamA;
TinyMQBridge bridgeA(upstreamA);
WiFiClient upstreamB;
TinyMQBridge bridgeB(upstreamB);

static IPAddress g_ipA, g_ipB;
static char g_line[256];
static size_t g_lineLen;

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
    if (!strcmp(cmd, "BROKER")) {
        server.begin((uint16_t)atoi(nextWord(&s)));
        Serial.println("OK BROKER");
    } else if (!strcmp(cmd, "BRIDGE")) {
        g_ipA.fromString(nextWord(&s));
        uint16_t port = (uint16_t)atoi(nextWord(&s));
        TinyMQClient &c = bridgeA.client();
        c.setServer(g_ipA, port);
        c.setClientId("tm-bridge-a");
        c.setProtocolVersion((uint8_t)atoi(nextWord(&s)));
        c.setKeepAlive(10);
        bridgeA.setReconnectInterval(500);
        bridgeA.addTopic("out/#", TinyMQBridge::kOut, 1, "", "edge1/");
        bridgeA.addTopic("cmd/#", TinyMQBridge::kIn, 1, "", "edge1/");
        bridgeA.addTopic("sync/#", TinyMQBridge::kBoth, 1);
        bridgeA.addTopic("ret/#", TinyMQBridge::kOut, 1, "", "edge1/");
        bridgeA.addTopic("+/temp", TinyMQBridge::kOut, 0, "sensors/", "edge1/s/");
        broker.addListener(bridgeA);
        Serial.println("OK BRIDGE");
    } else if (!strcmp(cmd, "BRIDGE2")) {
        g_ipB.fromString(nextWord(&s));
        uint16_t port = (uint16_t)atoi(nextWord(&s));
        TinyMQClient &c = bridgeB.client();
        c.setServer(g_ipB, port);
        c.setClientId("tm-bridge-b");
        c.setKeepAlive(10);
        bridgeB.setReconnectInterval(500);
        bridgeB.addTopic("o2/#", TinyMQBridge::kOut, 1, "", "edge2/");
        bridgeB.addTopic("c2/#", TinyMQBridge::kIn, 1, "", "edge2/");
        broker.addListener(bridgeB);
        Serial.println("OK BRIDGE2");
    } else if (!strcmp(cmd, "LPUB")) {
        uint8_t qos = (uint8_t)atoi(nextWord(&s));
        bool retain = atoi(nextWord(&s)) != 0;
        char *topic = nextWord(&s);
        Serial.println(broker.publish(topic, s, qos, retain) ? "LPUB ok" : "LPUB fail");
    } else if (!strcmp(cmd, "STATS")) {
        char out[160];
        snprintf(out, sizeof(out), "BSTATS a=%d out=%lu in=%lu dropped=%lu queued=%u b=%d out=%lu in=%lu", bridgeA.connected() ? 1 : 0,
                 (unsigned long)bridgeA.forwardedOut(), (unsigned long)bridgeA.forwardedIn(), (unsigned long)bridgeA.dropped(), (unsigned)bridgeA.queued(),
                 bridgeB.connected() ? 1 : 0,
                 (unsigned long)bridgeB.forwardedOut(), (unsigned long)bridgeB.forwardedIn());
        Serial.println(out);
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
    broker.begin();
    broker.addListener(listener);
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
