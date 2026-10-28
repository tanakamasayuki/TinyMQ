# Beginner's guide

[日本語](guide-beginner.ja.md) | [README](../README.md) | [Advanced guide](guide-advanced.md)

This guide starts by making an ESP32 an MQTT client and connecting it to a broker on your PC. It then makes the ESP32 the broker and connects a small microcontroller to it over MQTT-SN.

## 1. MQTT and TinyMQ

MQTT passes messages through a **broker** in the middle.

- The sender **publishes** a message with a **topic** (a name such as `sensors/room1/temp`)
- A receiver **subscribes** to the topics it wants
- The broker hands each message it gets to every client subscribed to that topic

Senders and receivers do not need to know about each other. Subscriptions can use the wildcards `+` (one level) and `#` (everything below). `sensors/+/temp` matches `sensors/room1/temp`, and `sensors/#` matches everything under `sensors`.

TinyMQ has these parts:

| Part | Role |
|---|---|
| `TinyMQClient` | MQTT client (TCP) |
| `TinyMQBroker` | MQTT broker |
| `TinyMQSnListener` | MQTT-SN gateway attached to the broker (UDP) |
| `TinyMQClientSn` | MQTT-SN client (UDP), for small microcontrollers |
| `TinyMQBridge` | Bridge from the broker to an upstream broker |

MQTT-SN is a standard for using MQTT over UDP on small devices. It uses 2-byte IDs instead of topic names, so packets are small, and it works without TCP.

## 2. What you need

- An ESP32 board (an ESP32-S3 or similar works the same) and the Arduino IDE or Arduino CLI, with arduino-esp32 3.3 or later
- A PC (Linux, macOS or Windows). You install the MQTT broker mosquitto on it
- For the later sections, a CH32V with Ethernet (CH32V307 or similar)

## 3. Install TinyMQ

Put it in your libraries folder.

```sh
cd ~/Arduino/libraries      # Documents\Arduino\libraries on Windows
git clone https://github.com/tanakamasayuki/TinyMQ
```

Restart the Arduino IDE. The examples appear under File → Examples → TinyMQ.

## 4. Set up a broker on the PC

Install mosquitto.

```sh
sudo apt install mosquitto mosquitto-clients   # Debian / Ubuntu
brew install mosquitto                          # macOS
```

By default, mosquitto 2.0 and later only accepts connections from the PC itself. To let other devices in, create a config file (for example `tinymq.conf`).

```
listener 1883
allow_anonymous true
```

This setting is for testing only (anyone can connect). Start it:

```sh
mosquitto -c tinymq.conf -v
```

Open another terminal and watch all topics.

```sh
mosquitto_sub -h localhost -t '#' -v
```

## 5. Make the ESP32 a client

Open the [`Esp32Client`](../examples/Esp32Client/README.md) example and edit these lines.

```cpp
static const char *kWifiSsid = "your-ssid";
static const char *kWifiPass = "your-passphrase";
static const IPAddress kBroker(192, 168, 1, 10);   // the PC's address
```

Upload it and open the Serial Monitor (115200 bps). You see:

```
TinyMQ Esp32Client
Local IP: 192.168.1.50
connected
```

In the `mosquitto_sub` terminal, a counter appears every 5 seconds.

```
tinymq/status online
tinymq/counter 0
tinymq/counter 1
```

Now send something from the PC to the board.

```sh
mosquitto_pub -h localhost -t tinymq/cmd -m hello
```

If `received tinymq/cmd: hello` appears on the serial port, it works.

### Inside the sketch

```cpp
WiFiClient net;              // the TCP connection (WiFiClientSecure for TLS)
TinyMQClient mqtt(net);

mqtt.setServer(kBroker, 1883);
mqtt.setClientId("esp32-client");   // a name unique within the broker
mqtt.onMessage(onMessage);          // called when a message arrives
mqtt.connect();                     // connects and waits for the reply
mqtt.subscribe("tinymq/cmd", 1);

// in loop()
mqtt.update();                      // receiving and keep alive; always call it
mqtt.publish("tinymq/counter", text, 1);
```

Without `update()`, nothing is received, and the keep alive runs out so the broker disconnects the client. Do not put long `delay()` calls in `loop()`.

## 6. QoS, retain and will

| Feature | What it does |
|---|---|
| QoS 0 | Fire and forget. The message may not arrive |
| QoS 1 | Resent until acknowledged. It may arrive twice |
| QoS 2 | Delivered exactly once. The heaviest |
| retain | The broker keeps the last value of the topic and gives it at once to clients that subscribe later |
| will | A message the broker sends on the client's behalf when the client disappears silently (power loss and so on) |

On connecting, `Esp32Client` publishes `online` to `tinymq/status` with retain, and registers `offline` as its will. Unplug the board, and when the keep alive runs out, `tinymq/status offline` appears in `mosquitto_sub`.

Sending an empty retained message to the same topic clears the stored value.

```sh
mosquitto_pub -h localhost -t tinymq/status -n -r
```

## 7. Make the ESP32 the broker

Now the ESP32 is the broker instead of the PC. Upload [`Esp32Broker`](../examples/Esp32Broker/README.md); its address appears on the serial port.

```
TinyMQ Esp32Broker
Broker at 192.168.1.20
```

Stop mosquitto on the PC and connect from the PC as a client.

```sh
mosquitto_sub -h 192.168.1.20 -t '#' -v
mosquitto_pub -h 192.168.1.20 -t sensors/temperature -m 21.5
```

The sketch itself subscribes under `sensors/`, so messages sent there also appear on the serial port.

```
client in:  mosquitto_pub_1234
sensors/temperature = 21.5
```

Set `kBroker` in the `Esp32Client` from §5 to this board's address, and the two boards connect to each other.

### Inside the sketch

```cpp
TinyMQBroker broker;
TinyMQLwipListener tcp(1883);   // the TCP listener
WiFiUDP udp;
TinyMQSnListener sn(udp);       // the MQTT-SN listener (UDP)

broker.begin();
tcp.begin();
udp.begin(1884);
broker.addListener(tcp);
broker.addListener(sn);
broker.onMessage(onMessage);    // messages for the sketch's own subscriptions
broker.subscribe("sensors/#");

// in loop()
broker.update();                // that's all
```

You use the broker by registering listeners with it. MQTT over TCP, MQTT-SN over UDP and the bridge to an upstream broker all share the topics of the same broker.

## 8. Connect a small microcontroller over MQTT-SN

On a microcontroller with little RAM, such as a CH32V, use the MQTT-SN client `TinyMQClientSn`. In [`Ch32SnSensor`](../examples/Ch32SnSensor/README.md), set `kGateway` to the address of the board from §7 and upload it.

```cpp
static const IPAddress kGateway(192, 168, 1, 20);
```

In the `mosquitto_sub -h 192.168.1.20 -t '#' -v` terminal, a value appears every 10 seconds.

```
sensors/uptime 10
sensors/uptime 20
```

Turn the LED on from the PC.

```sh
mosquitto_pub -h 192.168.1.20 -t cmd/led -m on
```

### Sending without connecting (QoS −1)

MQTT-SN has QoS −1, which sends without connecting. The topic is given either as an ID agreed with the gateway in advance (a predefined topic) or as a 2-character short topic name. `Esp32Broker` maps ID 1 to `sensors/temperature`.

```cpp
sn.setGateway(kGateway, 1884);
sn.publishPredefined(1, "21.5");   // no connection needed
```

For a send-only device, this is all you need. Built with `-DTINYMQ_SN_CLIENT_LEVEL=0`, it leaves out everything else and takes about 1 KB of Flash.

## 9. Security

- With the settings so far, anyone can connect. For real use, add authentication (`broker.onAuthenticate()`) and authorization (`broker.onAuthorize()`) to the broker. See [advanced guide](guide-advanced.md) §9
- MQTT-SN has no encryption and no authentication. Use it on a closed network, or inside a WireGuard tunnel from [TinyWG](https://github.com/tanakamasayuki/TinyWG)
- To encrypt a TCP client, pass a `WiFiClientSecure` instead of a `WiFiClient` and set the broker's certificate

## 10. Troubleshooting

| Symptom | Where to look |
|---|---|
| `connect failed, reason 0x80` | The broker's address and port, the PC's firewall, and whether mosquitto listens on all addresses (`listener 1883`) |
| `connect failed, reason 0x87` | The broker requires authentication (`allow_anonymous`, user name and password) |
| `connect failed, reason 0x85` | The Client ID is empty or not accepted |
| Connects, then drops at once | Is `update()` called in `loop()`? Is a `delay()` too long? Is another device using the same Client ID? (The one that connected later stays) |
| Messages do not arrive | The topic's spelling and the subscription's wildcards. Use `mosquitto_sub -t '#' -v` to see what actually flows |
| Only large messages do not arrive | Anything larger than the client's receive buffer (`TINYMQ_MAX_PACKET`, 1024 bytes by default) is dropped |
| MQTT-SN does not connect | The gateway's address and port (1884), and whether the broker side calls `udp.begin(1884)` and `addListener(sn)` |

The meaning of the `lastReason()` values is in the [API reference](api.md#reason-codes).
