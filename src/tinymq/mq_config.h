// Build-time settings. Override any of these with -D.
#pragma once

// Presets: -DTINYMQ_PRESET_SMALL picks smaller defaults for the broker and
// the MQTT-SN gateway, for boards whose network stack leaves little RAM
// (CH32V307 with Ethernet). The number of connections and the largest packet
// stay the same; fewer sessions, subscriptions and retained messages, and
// shorter queues. Any setting below can still be given with -D.
#ifdef TINYMQ_PRESET_SMALL
#ifndef TINYMQ_BROKER_MAX_SESSIONS
#define TINYMQ_BROKER_MAX_SESSIONS 8
#endif
#ifndef TINYMQ_BROKER_MAX_SUBSCRIPTIONS
#define TINYMQ_BROKER_MAX_SUBSCRIPTIONS 32
#endif
#ifndef TINYMQ_BROKER_MAX_RETAINED
#define TINYMQ_BROKER_MAX_RETAINED 16
#endif
#ifndef TINYMQ_BROKER_MAX_QUEUED
#define TINYMQ_BROKER_MAX_QUEUED 16
#endif
#ifndef TINYMQ_BROKER_MAX_QOS2_IN
#define TINYMQ_BROKER_MAX_QOS2_IN 8
#endif
#ifndef TINYMQ_BROKER_POOL_BYTES
#define TINYMQ_BROKER_POOL_BYTES 8192
#endif
#ifndef TINYMQ_SN_MAX_CLIENTS
#define TINYMQ_SN_MAX_CLIENTS 4
#endif
#ifndef TINYMQ_SN_MAX_TOPICS
#define TINYMQ_SN_MAX_TOPICS 32
#endif
#ifndef TINYMQ_SN_POOL_BYTES
#define TINYMQ_SN_POOL_BYTES 2048
#endif
#endif

// Longest topic name accepted or sent, in bytes
#ifndef TINYMQ_MAX_TOPIC_LEN
#define TINYMQ_MAX_TOPIC_LEN 128
#endif

// Longest packet accepted, in bytes (fixed header included)
#ifndef TINYMQ_MAX_PACKET
#define TINYMQ_MAX_PACKET 1024
#endif

// ---- client

// Bytes kept for unacknowledged QoS 1 / 2 messages (resent on reconnect)
#ifndef TINYMQ_CLIENT_POOL_BYTES
#define TINYMQ_CLIENT_POOL_BYTES 2048
#endif

// Outgoing QoS 1 / 2 messages awaiting acknowledgement
#ifndef TINYMQ_CLIENT_MAX_INFLIGHT
#define TINYMQ_CLIENT_MAX_INFLIGHT 8
#endif

// Incoming QoS 2 packet identifiers awaiting PUBREL (duplicate detection)
#ifndef TINYMQ_CLIENT_MAX_QOS2_IN
#define TINYMQ_CLIENT_MAX_QOS2_IN 8
#endif

// ---- broker

// Concurrent connections (all listeners together)
#ifndef TINYMQ_BROKER_MAX_CLIENTS
#define TINYMQ_BROKER_MAX_CLIENTS 8
#endif

// Sessions kept, including persistent sessions of disconnected clients
#ifndef TINYMQ_BROKER_MAX_SESSIONS
#define TINYMQ_BROKER_MAX_SESSIONS 16
#endif

// Subscriptions of all sessions and of the local client together
#ifndef TINYMQ_BROKER_MAX_SUBSCRIPTIONS
#define TINYMQ_BROKER_MAX_SUBSCRIPTIONS 64
#endif

// Retained messages
#ifndef TINYMQ_BROKER_MAX_RETAINED
#define TINYMQ_BROKER_MAX_RETAINED 32
#endif

// Messages waiting for or in delivery, per session
#ifndef TINYMQ_BROKER_MAX_QUEUED
#define TINYMQ_BROKER_MAX_QUEUED 32
#endif

// Of those, QoS 1 / 2 messages sent and not yet acknowledged, per session
#ifndef TINYMQ_BROKER_MAX_INFLIGHT
#define TINYMQ_BROKER_MAX_INFLIGHT 8
#endif

// Incoming QoS 2 packet identifiers awaiting PUBREL, per session
#ifndef TINYMQ_BROKER_MAX_QOS2_IN
#define TINYMQ_BROKER_MAX_QOS2_IN 16
#endif

// Receive buffer per connection: the largest packet a client can send
#ifndef TINYMQ_BROKER_RX_BUFFER
#define TINYMQ_BROKER_RX_BUFFER TINYMQ_MAX_PACKET
#endif

// Message pool for retained, queued and will messages, client identifiers
// and topic filters. 0: allocated once by begin().
#ifndef TINYMQ_BROKER_POOL_BYTES
#define TINYMQ_BROKER_POOL_BYTES 16384
#endif

// Accept MQTT 5.0 clients
#ifndef TINYMQ_ENABLE_MQTT5
#define TINYMQ_ENABLE_MQTT5 1
#endif

// Shared subscriptions ($share/<group>/<filter>)
#ifndef TINYMQ_ENABLE_SHARED_SUB
#define TINYMQ_ENABLE_SHARED_SUB 1
#endif

// Topic aliases a 5.0 client may use towards the broker, per connection
// (0: none)
#ifndef TINYMQ_BROKER_TOPIC_ALIAS_MAX
#define TINYMQ_BROKER_TOPIC_ALIAS_MAX 8
#endif

// Internal subscribers for bridges (TinyMQBridge), one per bridge
#ifndef TINYMQ_BROKER_MAX_TAPS
#define TINYMQ_BROKER_MAX_TAPS 2
#endif

// Include QoS 2 delivery to subscribers (receiving QoS 2 is always supported)
#ifndef TINYMQ_ENABLE_QOS2_DELIVERY
#define TINYMQ_ENABLE_QOS2_DELIVERY 1
#endif

// ---- MQTT-SN gateway (TinyMQSnListener)

// MQTT-SN clients at once; each also takes one of TINYMQ_BROKER_MAX_CLIENTS
#ifndef TINYMQ_SN_MAX_CLIENTS
#define TINYMQ_SN_MAX_CLIENTS 8
#endif

// Topic id registrations of all MQTT-SN clients together
#ifndef TINYMQ_SN_MAX_TOPICS
#define TINYMQ_SN_MAX_TOPICS 64
#endif

// Predefined topic ids
#ifndef TINYMQ_SN_MAX_PREDEFINED
#define TINYMQ_SN_MAX_PREDEFINED 16
#endif

// Bytes for registered topic names and wills being connected
#ifndef TINYMQ_SN_POOL_BYTES
#define TINYMQ_SN_POOL_BYTES 4096
#endif

// ---- MQTT-SN client (TinyMQClientSn)

// 0: PUBLISH with QoS -1 only; 1: connect, register, publish QoS 0 / 1,
// subscribe, keep alive; 2: also QoS 2, will, sleep, gateway search.
// Functions a sketch never calls are dropped by the linker at any level;
// lower levels make update() and the buffers smaller.
#ifndef TINYMQ_SN_CLIENT_LEVEL
#define TINYMQ_SN_CLIENT_LEVEL 2
#endif

// Largest datagram sent or received
#ifndef TINYMQ_SN_CLIENT_BUFFER
#define TINYMQ_SN_CLIENT_BUFFER 256
#endif

// Topic ids remembered (own registrations, SUBACK, the gateway's REGISTER)
#ifndef TINYMQ_SN_CLIENT_MAX_TOPICS
#define TINYMQ_SN_CLIENT_MAX_TOPICS 8
#endif

// Longest topic name remembered
#ifndef TINYMQ_SN_CLIENT_TOPIC_LEN
#define TINYMQ_SN_CLIENT_TOPIC_LEN 32
#endif

// ---- bridge (TinyMQBridge)

// Topic map entries per bridge
#ifndef TINYMQ_BRIDGE_MAX_TOPICS
#define TINYMQ_BRIDGE_MAX_TOPICS 8
#endif

// QoS 1 / 2 messages a bridge keeps for upstream while it is down:
// bytes (0: none kept) and count
#ifndef TINYMQ_BRIDGE_QUEUE_BYTES
#define TINYMQ_BRIDGE_QUEUE_BYTES 2048
#endif
#ifndef TINYMQ_BRIDGE_MAX_QUEUED
#define TINYMQ_BRIDGE_MAX_QUEUED 16
#endif
