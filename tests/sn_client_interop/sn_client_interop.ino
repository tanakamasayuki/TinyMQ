// TinyMQClientSn driven by serial commands, talking over real UDP sockets to
// the TinyMQBroker gateway running in the same sketch (or to a scripted
// gateway in the test). While a client procedure waits, the idle hook keeps
// the broker running. TCP clients in the test check what arrives.
//
// Commands (one per line):
//   BROKER <tcp port> <udp port>   GW <ip> <port>   ID <id>   KEEPALIVE <s>
//   CLEAN <0|1>   RETRY <ms> <count>   WILL <qos> <retain> <topic> <payload>   NOWILL
//   CONNECT   DISCONNECT   REG <topic>   PUB <qos> <retain> <topic> <payload>
//   PUBPRE <id> <qos> <payload>   PUBSHORT <name> <qos> <payload>
//   SUB <qos> <filter>   SUBPRE <id> <qos>   UNSUB <filter>
//   SLEEP <s>   CHECK   SEARCH <ip> <port>   HOLD <ms> (the client stays silent)
//   STATUS
// Events printed:
//   MSG <topic or -> <id> <type> <qos> <retain> <dup> <payload>

#include <TinyMQ.h>

#include "board_net.h"

WiFiServer server;
TinyMQTcpListener<WiFiServer> listener(server);
WiFiUDP gatewayUdp;
TinyMQSnListener gateway(gatewayUdp);
TinyMQBroker broker;

WiFiUDP clientUdp;
TinyMQClientSn sn(clientUdp);

static char g_id[24] = "dut-sn";
static char g_willTopic[64];
static char g_willPayload[64];
static char g_line[512];
static size_t g_lineLen;
static bool g_brokerOn;

static void runBroker(void *)
{
    if (g_brokerOn)
        broker.update();
}

static void onMessage(const tinymq::SnMessage &m, void *)
{
    Serial.print("MSG ");
    Serial.print(m.topic ? m.topic : "-");
    Serial.print(' ');
    Serial.print(m.topicId);
    Serial.print(' ');
    Serial.print(m.topicType);
    Serial.print(' ');
    Serial.print(m.qos);
    Serial.print(' ');
    Serial.print(m.retain ? 1 : 0);
    Serial.print(' ');
    Serial.print(m.dup ? 1 : 0);
    Serial.print(' ');
    Serial.write(m.payload, m.payloadLen < 200 ? m.payloadLen : 200);
    Serial.println();
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

static void result(const char *what, bool ok)
{
    Serial.print(what);
    Serial.print(ok ? " ok " : " fail ");
    Serial.println(sn.lastReturnCode());
}

static void command(char *line)
{
    char *s = line;
    char *cmd = nextWord(&s);
    if (!strcmp(cmd, "BROKER")) {
        uint16_t tcp = (uint16_t)atoi(nextWord(&s));
        uint16_t udp = (uint16_t)atoi(nextWord(&s));
        server.begin(tcp);
        gatewayUdp.begin(udp);
        g_brokerOn = true;
        Serial.println("BROKER ready");
    } else if (!strcmp(cmd, "GW")) {
        IPAddress ip;
        ip.fromString(nextWord(&s));
        sn.setGateway(ip, (uint16_t)atoi(nextWord(&s)));
        Serial.println("OK GW");
    } else if (!strcmp(cmd, "ID")) {
        strncpy(g_id, nextWord(&s), sizeof(g_id) - 1);
        sn.setClientId(g_id);
        Serial.println("OK ID");
    } else if (!strcmp(cmd, "KEEPALIVE")) {
        sn.setKeepAlive((uint16_t)atoi(nextWord(&s)));
        Serial.println("OK KEEPALIVE");
    } else if (!strcmp(cmd, "CLEAN")) {
        sn.setCleanSession(atoi(nextWord(&s)) != 0);
        Serial.println("OK CLEAN");
    } else if (!strcmp(cmd, "RETRY")) {
        uint32_t ms = (uint32_t)atol(nextWord(&s));
        sn.setRetry(ms, (uint8_t)atoi(nextWord(&s)));
        Serial.println("OK RETRY");
    } else if (!strcmp(cmd, "WILL")) {
        uint8_t qos = (uint8_t)atoi(nextWord(&s));
        bool retain = atoi(nextWord(&s)) != 0;
        strncpy(g_willTopic, nextWord(&s), sizeof(g_willTopic) - 1);
        strncpy(g_willPayload, s, sizeof(g_willPayload) - 1);
        sn.setWill(g_willTopic, g_willPayload, qos, retain);
        Serial.println("OK WILL");
    } else if (!strcmp(cmd, "NOWILL")) {
        sn.clearWill();
        Serial.println("OK NOWILL");
    } else if (!strcmp(cmd, "CONNECT")) {
        result("CONNECT", sn.connect());
    } else if (!strcmp(cmd, "DISCONNECT")) {
        sn.disconnect();
        Serial.println("DISCONNECT done");
    } else if (!strcmp(cmd, "REG")) {
        Serial.print("REG ");
        Serial.println(sn.registerTopic(nextWord(&s)));
    } else if (!strcmp(cmd, "PUB")) {
        uint8_t qos = (uint8_t)atoi(nextWord(&s));
        bool retain = atoi(nextWord(&s)) != 0;
        char *topic = nextWord(&s);
        result("PUB", sn.publish(topic, s, qos, retain));
    } else if (!strcmp(cmd, "PUBPRE")) {
        uint16_t id = (uint16_t)atoi(nextWord(&s));
        int8_t qos = (int8_t)atoi(nextWord(&s));
        result("PUBPRE", sn.publishPredefined(id, s, qos));
    } else if (!strcmp(cmd, "PUBSHORT")) {
        char *name = nextWord(&s);
        int8_t qos = (int8_t)atoi(nextWord(&s));
        result("PUBSHORT", sn.publishShort(name, s, qos));
    } else if (!strcmp(cmd, "SUB")) {
        uint8_t qos = (uint8_t)atoi(nextWord(&s));
        int granted = sn.subscribe(nextWord(&s), qos);
        Serial.print("SUB ");
        Serial.println(granted);
    } else if (!strcmp(cmd, "SUBPRE")) {
        uint16_t id = (uint16_t)atoi(nextWord(&s));
        int granted = sn.subscribePredefined(id, (uint8_t)atoi(nextWord(&s)));
        Serial.print("SUBPRE ");
        Serial.println(granted);
    } else if (!strcmp(cmd, "UNSUB")) {
        result("UNSUB", sn.unsubscribe(nextWord(&s)));
    } else if (!strcmp(cmd, "SLEEP")) {
        result("SLEEP", sn.sleep((uint16_t)atoi(nextWord(&s))));
    } else if (!strcmp(cmd, "CHECK")) {
        result("CHECK", sn.checkMessages());
    } else if (!strcmp(cmd, "SEARCH")) {
        IPAddress ip;
        ip.fromString(nextWord(&s));
        uint16_t port = (uint16_t)atoi(nextWord(&s));
        Serial.print("SEARCH ");
        Serial.println(sn.searchGateway(ip, port, 1, 3000));
    } else if (!strcmp(cmd, "HOLD")) {
        uint32_t until = millis() + (uint32_t)atol(nextWord(&s));
        while ((int32_t)(millis() - until) < 0) {
            runBroker(nullptr);
            delay(1);
        }
        Serial.println("HOLD done");
    } else if (!strcmp(cmd, "STATUS")) {
        Serial.print("STATUS connected=");
        Serial.print(sn.connected() ? 1 : 0);
        Serial.print(" asleep=");
        Serial.println(sn.asleep() ? 1 : 0);
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
    broker.addListener(gateway);
    gateway.addPredefinedTopic(1, "pre/one");
    gateway.addPredefinedTopic(2, "pre/two");
    clientUdp.begin(0);
    sn.setClientId(g_id);
    sn.onMessage(onMessage);
    sn.onIdle(runBroker);
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
    sn.update();
    runBroker(nullptr);
    delay(1);
}
