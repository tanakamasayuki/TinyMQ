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
