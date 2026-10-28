# Esp32Persist

[日本語](README.ja.md) | [Advanced guide](../../docs/guide-advanced.md)

A broker that keeps retained messages and persistent sessions (subscriptions and QoS 1 / 2 messages not yet delivered) in LittleFS across restarts. It saves 2 s after the retained messages change, and every 5 minutes.

## What you need

- An ESP32 board (arduino-esp32 3.3 or later) with a LittleFS partition

## Settings

| Constant | Meaning |
|---|---|
| `kWifiSsid` / `kWifiPass` | Wi-Fi |
| `kStateFile` | The file it saves to |

## What you see

```
TinyMQ Esp32Persist
Broker at 192.168.1.20
state loaded
state saved
```

After `mosquitto_pub -h 192.168.1.20 -t room/temp -m 21 -r`, reset the board: `mosquitto_sub -h 192.168.1.20 -t room/temp` still gets `21`.

## Notes

- It writes a new file and then replaces the old one, so a reset while saving leaves the previous state.
- QoS 1 / 2 messages in flight when saved are delivered again from the start after loading (QoS 1 may arrive twice). Delayed wills are not saved.
- Choose the save interval with the flash's write endurance in mind.
