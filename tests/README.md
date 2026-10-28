# Tests

[日本語](README.ja.md)

As with TinyWG, the tests run on [pytest-embedded](https://docs.espressif.com/projects/pytest-embedded/en/latest/) + Arduino CLI ([pytest-embedded-arduino-cli](https://github.com/tanakamasayuki/pytest-embedded-arduino-cli)). The default profile is `lang-ship:host` (sketches that run on the PC).

```sh
cd tests
uv run pytest smoke codec topic pool sn_codec props
uv run pytest client_interop          # needs mosquitto (below)
uv run pytest broker_interop sn_client_interop bridge_interop
```

## mosquitto

The interoperability tests run against mosquitto on the PC. It is looked up in `$MOSQUITTO`, `PATH` (including `/usr/sbin`) and `tests/.tools/mosquitto`, in that order; if it is not found, the tests are skipped. Without root, on Debian / Ubuntu, the following command unpacks the package into `tests/.tools/mosquitto` for use (it does not install anything).

```sh
tests/tools/fetch_mosquitto.sh
```

Each test starts mosquitto on a free port, and the configuration and log are kept in that test's `output/`.

## Tests

| Directory | Contents | Profiles |
|---|---|---|
| `smoke/` | Checks the build and the harness | host / esp32 / esp32s3 / ch32v307 / picow |
| `codec/` | Variable byte integers, UTF-8 validation, the fixed header, and encoding / decoding of every packet type (3.1.1 and 5.0), checked against byte sequences built from the specification. Rejection of malformed packets, buffer too small | Same as above |
| `sn_codec/` | MQTT-SN 1.2 flags, the short and long length forms (the 256-octet boundary), and every message the gateway and the client use, checked against byte sequences built from the specification. Rejection of malformed messages | host / esp32 / esp32s3 / ch32v307 / picow |
| `topic/` | Validation of topic names and filters, matching (the examples in §4.7 of the specification), topics starting with `$`, splitting of shared subscriptions `$share/` | Same as above |
| `client_interop/` | Drives `TinyMQClient` (3.1.1 / 3.1 / 5.0) with serial commands (5.0 is in `client_v5_scenarios.py`: sending and receiving properties, subscription identifiers, No Local, session expiry, the assigned Client ID and reason codes, will on DISCONNECT, Will Delay, DISCONNECT on takeover, following the CONNACK properties (Receive Maximum, Maximum QoS, Retain Available, Server Keep Alive, Maximum Packet Size), DISCONNECT on a topic alias that was not allowed, DISCONNECT from the server and rejection with a reason code). Against mosquitto: QoS 0 / 1 / 2 send and receive and QoS downgrade, retain, unsubscribe, will (not sent on DISCONNECT / sent on a dropped connection), persistent sessions, keepalive, discarding messages larger than the receive buffer, MQTT 3.1, user name and password. Against a scripted fake broker: QoS 1 resend (DUP) and PUBREL resend after reconnecting, no resend when there is no session, QoS 2 duplicate receive, disconnecting when PINGRESP does not come, connection refused | host / esp32 / esp32s3 / picow |
| `broker_interop/` | Drives `TinyMQBroker` + `TinyMQTcpListener<WiFiServer>` with serial commands (5.0 is in `broker_v5_scenarios.py`: CONNACK properties and the assigned Client ID, rejection of enhanced authentication, relaying properties, subscription identifiers, No Local / Retain As Published / Retain Handling, message expiry, session expiry, Will Delay, topic aliases, reason codes, shared subscriptions, Receive Maximum and Maximum Packet Size, DISCONNECT from the server), with paho-mqtt and raw-socket clients connecting. QoS 0 / 1 / 2 and overlapping subscriptions (once, at the highest QoS), the maximum QoS limit, wildcards and `$` topics, retain (flag cleared when forwarded, resent on resubscribing with the same filter, deleted by an empty message), will (sent on a dropped connection or keepalive timeout / not sent on DISCONNECT), persistent sessions (QoS 0 is not kept), takeover by the same Client ID, resend with DUP and PUBREL resend after reconnecting, QoS 2 duplicate receive, authentication, per-topic authorization, local clients, unsubscribe, protocol violations (packets before CONNECT, unknown level, empty Client ID, a second CONNECT, reserved flags, packets that are too large, CONNECT timeout), MQTT 3.1, the connection limit, payload length; on real boards the TCP and 5.0 scenarios run again over the lwIP listener (`TinyMQLwipListener`, port 1885). `$SYS` topics, persistence (simulating a restart with `saveState()` → `end()` → `loadState()`, and checking that retained messages, persistent sessions and undelivered messages, and 5.0 subscription identifiers and shared subscriptions come back), and at the end a check that everything has been freed. On the same DUT, the MQTT-SN gateway (`TinyMQSnListener`) is also tested from an MQTT-SN client written in Python (`helpers/sn_peer.py`) (`broker_sn_scenarios.py`): QoS 0 / 1 / 2 send and receive and delivery both ways with the TCP side, topic name registration, REGISTER from the gateway on wildcard subscriptions, short topic names, predefined topics, unknown topic IDs, QoS -1 without a connection, will (the WILLTOPIC / WILLMSG exchange, WILLTOPICUPD / WILLMSGUPD, sent on keepalive timeout), keepalive, sleep (messages received while asleep are delivered on wake-up, followed by PINGRESP), resend over UDP (DUP) and disconnecting clients that do not respond, QoS 2 duplicate receive, persistent sessions, CONNECT resend, SEARCHGW | host / esp32 / esp32s3 / picow |
| `sn_client_interop/` | Drives `TinyMQClientSn` with serial commands. It connects over a real UDP socket to the gateway of a broker running in the same sketch (while waiting, `onIdle()` runs the broker), and the results are checked from the TCP side with paho-mqtt: QoS -1 without a connection, QoS 0 / 1 / 2 send (registration is automatic), retain, predefined and short topic names, subscriptions (REGISTER from the gateway on wildcards), will and keepalive timeout, sleep and receiving on wake-up, CONNECT from sleep. Against a scripted fake gateway: resend with DUP, registering again on "invalid topic ID", failure when there is no response, rejection of unknown topic IDs, QoS 2 duplicate receive, DISCONNECT from the gateway, connection refused, gateway discovery | host / esp32 / esp32s3 / picow |
| `props/` | Building, reading and validating MQTT 5.0 properties (types, value ranges, IDs allowed per packet, duplicates, UTF-8), copying with given IDs left out | host / esp32 / esp32s3 / ch32v307 / picow |
| `bridge_interop/` | A broker with two `TinyMQBridge` instances (5.0 and 3.1.1) bridging to mosquitto on the PC. Sending retained messages from before the bridge to upstream, out / in / both forwarding and prefix rewriting, the QoS limit, relaying 5.0 properties, no loops with both, discarding while upstream is down, reconnecting to an upstream that comes back on the same port (subscriptions and retained messages are redone) | host / esp32 / esp32s3 / picow |
| `broker_conformance/` | Runs the Eclipse paho.mqtt.testing interoperability test suite (3.1.1 and 5.0) against `TinyMQBroker`. Since it runs external code, it is fetched with `tools/fetch_paho_testing.sh` and runs only when `TINYMQ_RUN_PAHO_TESTING=1` is set. **Not yet run end to end** | host / esp32 / esp32s3 / picow |
| `bench_broker/` | Broker measurements. From paho-mqtt on the PC, per listener: round-trip latency (QoS 0 / 1), one-to-one throughput and one-to-four fan-out. Printed as `BENCH <name> <value> <unit>` and kept as junit properties. Fails only when QoS 1 messages go missing | host / esp32 / esp32s3 / picow |
| `pool/` | Message pool allocation, freeing, merging, exhaustion and alignment, 20,000 random allocations and frees, reference-counted messages | Same as above |

## Running on real boards

Copy `.env.example` to `.env` and fill in the serial ports and Wi-Fi. Wi-Fi is passed to the sketches as `WIFI_SSID` / `WIFI_PASSWORD` through `build_config.toml`.

```sh
uv run --env-file .env pytest smoke codec topic pool sn_codec --profiles=esp32,esp32s3
uv run --env-file .env pytest broker_interop client_interop sn_client_interop --profile esp32 -s
uv run --env-file .env pytest --profiles=all -rs     # host and every connected board
```

`client_interop` / `broker_interop` / `sn_client_interop` run the same scenarios on host and on the Wi-Fi boards (esp32 / esp32s3 / picow). The sketch connects to Wi-Fi and prints `READY <IP>`, and the test uses it to decide what to connect to.

- `broker_interop`: the board is the broker (TCP 1883, MQTT-SN 1884). paho-mqtt / raw sockets / the MQTT-SN peer on the PC connect to the board's IP
- `client_interop`: mosquitto and the scripted fake broker on the PC listen on all addresses, and the board connects to the PC's address (found from the route to the board)
- `sn_client_interop`: the broker, gateway and MQTT-SN client all run on the board, and the client connects to the gateway over loopback (127.0.0.1). The scripted fake gateway runs on the PC

The PC and the board must be on the same network and able to reach each other (do not block the mosquitto ports in the PC's firewall).

`output/` is cleared before each test by `arduino_reset_dirs` in `pyproject.toml`.
