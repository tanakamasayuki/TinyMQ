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
