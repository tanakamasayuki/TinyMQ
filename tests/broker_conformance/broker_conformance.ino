// TinyMQBroker for the Eclipse paho.mqtt.testing interoperability suites
// (test_broker_conformance.py). Subscriptions to "test/nosubscribe" are
// refused, as the suites expect.
//
// Commands: LISTEN <port>  ->  LISTENING <port>

#include <TinyMQ.h>

#include "board_net.h"

WiFiServer server;
TinyMQTcpListener<WiFiServer> listener(server);
TinyMQBroker broker;

static bool onAcl(tinymq::Bytes, tinymq::Bytes, tinymq::Bytes topic, bool subscribe, void *)
{
    return !(subscribe && topic.equals("test/nosubscribe"));
}

void setup()
{
    Serial.begin(115200);
    if (!networkReady())
        return;
    broker.begin();
    broker.addListener(listener);
    broker.onAuthorize(onAcl);
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
                server.begin((uint16_t)atoi(line + 7));
                Serial.print("LISTENING ");
                Serial.println(line + 7);
            }
            len = 0;
        } else if (len + 1 < sizeof(line)) {
            line[len++] = (char)c;
        }
    }
    broker.update();
    delay(1);
}
