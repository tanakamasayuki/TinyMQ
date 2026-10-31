// TinyMQBroker for the measurements of test_bench_broker.py: listens on
// the Arduino WiFiServer (TinyMQTcpListener) and, on boards with lwIP, on
// lwIP's raw API too (TinyMQLwipListener, port + 1).
//
// Commands: LISTEN <port>  ->  LISTENING <port> [lwip=<port>]
//   STATS  ->  STATS pool=<used> dropped=<n> clients=<n> broker=<sizeof> [heap=<free bytes>]

#include <TinyMQ.h>

#include "board_net.h"

WiFiServer server;
TinyMQTcpListener<WiFiServer> listener(server);
TinyMQBroker broker;
#if defined(TINYMQ_HAS_LWIP) && defined(BOARD_NET_REAL)
TinyMQLwipListener *lwipListener;
#endif

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
    static char line[64];
    static size_t len = 0;
    while (Serial.available()) {
        int c = Serial.read();
        if (c == '\r')
            continue;
        if (c == '\n') {
            line[len] = 0;
            if (!strncmp(line, "LISTEN ", 7)) {
                uint16_t port = (uint16_t)atoi(line + 7);
                server.begin(port);
                Serial.print("LISTENING ");
                Serial.print(port);
#if defined(TINYMQ_HAS_LWIP) && defined(BOARD_NET_REAL)
                lwipListener = new TinyMQLwipListener(port + 1);
                if (lwipListener->begin()) {
                    broker.addListener(*lwipListener);
                    Serial.print(" lwip=");
                    Serial.print(port + 1);
                }
#endif
                Serial.println();
            } else if (!strcmp(line, "STATS")) {
                Serial.print("STATS pool=");
                Serial.print((unsigned)broker.poolUsed());
                Serial.print(" dropped=");
                Serial.print((unsigned long)broker.droppedMessages());
                Serial.print(" clients=");
                Serial.print((unsigned)broker.engine().clients());
                Serial.print(" broker=");
                Serial.print((unsigned)sizeof(broker));
#if defined(ARDUINO_ARCH_ESP32)
                Serial.print(" heap=");
                Serial.print((unsigned long)ESP.getFreeHeap());
#elif defined(ARDUINO_ARCH_RP2040)
                Serial.print(" heap=");
                Serial.print((unsigned long)rp2040.getFreeHeap());
#endif
                Serial.println();
            }
            len = 0;
        } else if (len + 1 < sizeof(line)) {
            line[len++] = (char)c;
        }
    }
    broker.update();
}
