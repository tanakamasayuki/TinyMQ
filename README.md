# TinyMQ

[日本語](README.ja.md)

A portable MQTT library for Arduino with **both a client and a broker**.

- `TinyMQClient`: MQTT 3.1.1 / 5.0 over any Arduino `Client`
- `TinyMQClientSn`: MQTT-SN 1.2 over any Arduino `UDP`, for chips too small for TCP
- `TinyMQBroker`: relays MQTT (TCP) and MQTT-SN (UDP) clients in one topic space; features can be stripped to make it lighter

> Work in progress. See [SPEC.md](SPEC.md) for the design.

## License

MIT
