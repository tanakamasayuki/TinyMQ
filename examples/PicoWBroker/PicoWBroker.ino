// en: An MQTT broker on a Pico W / Pico 2 W, through the Arduino WiFiServer (TinyMQTcpListener).
// ja: Pico W / Pico 2 W で動く MQTT ブローカー。Arduino の WiFiServer の上で受け付ける(TinyMQTcpListener)。
#include <WiFi.h>
#include <TinyMQ.h>

static const char *kWifiSsid = "your-ssid";
static const char *kWifiPass = "your-passphrase";

TinyMQBroker broker;
WiFiServer server(1883);
TinyMQTcpListener<WiFiServer> tcp(server);

static void onMessage(const tinymq::Message &m, void *)
{
    Serial.print(m.topic);
    Serial.print(" = ");
    Serial.write(m.payload, m.payloadLen);
    Serial.println();
}

void setup()
{
    Serial.begin(115200);
    delay(500);
    Serial.println("TinyMQ PicoWBroker");

    WiFi.mode(WIFI_STA);
    WiFi.begin(kWifiSsid, kWifiPass);
    while (WiFi.status() != WL_CONNECTED) {
        delay(200);
        Serial.print('.');
    }
    Serial.print("\nBroker at ");
    Serial.println(WiFi.localIP());

    broker.begin();
    server.begin();
    broker.addListener(tcp);
    broker.onMessage(onMessage);
    broker.subscribe("#");
}

void loop()
{
    broker.update();
}
