// Build-time settings. Override any of these with -D.
#pragma once

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
