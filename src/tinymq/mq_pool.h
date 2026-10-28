// A byte arena for variable-length data (messages, topic filters, wills),
// and reference-counted messages on top of it.
//
// The arena is given once in begin() and never grows. Blocks are found
// first-fit and merged with free neighbours as they are freed and scanned.
// A handle is the offset of the block's data in the arena.
#pragma once

#include "mq_types.h"

namespace tinymq {

class Pool {
public:
    static const uint32_t kNull = 0xFFFFFFFF;

    // `mem` must stay valid while the pool is used. Anything beyond the
    // first 4-byte boundary is used; at most 2 GB.
    void begin(void *mem, size_t size);

    uint32_t alloc(size_t n);  // kNull if no free block is large enough
    void free(uint32_t h);
    uint8_t *ptr(uint32_t h) const { return base_ + h; }

    size_t capacity() const { return cap_; }
    size_t used() const { return used_; }   // including block headers
    size_t largestFree();                   // largest n that alloc(n) would accept now
    uint32_t blocks() const { return blocks_; }

private:
    uint32_t &header(uint32_t off) const { return *reinterpret_cast<uint32_t *>(base_ + off); }
    void mergeFrom(uint32_t off);

    uint8_t *base_ = nullptr;
    uint32_t cap_ = 0;
    uint32_t used_ = 0;
    uint32_t blocks_ = 0;
};

// A message kept for later delivery: topic, 5.0 properties, payload, QoS and
// retain flag. Created with one reference; freed when the last is released.
class MessageStore {
public:
    explicit MessageStore(Pool &pool) : pool_(pool) {}

    uint32_t create(Bytes topic, Bytes props, Bytes payload, uint8_t qos, bool retain, uint32_t expiry = 0);
    // As create(), leaving room for a payload of `payloadLen` bytes to be
    // filled through `payload`
    uint32_t createEmpty(Bytes topic, Bytes props, size_t payloadLen, uint8_t qos, bool retain, uint32_t expiry, uint8_t *&payload);
    void addRef(uint32_t h);
    void release(uint32_t h);

    Bytes topic(uint32_t h) const;
    Bytes props(uint32_t h) const;
    Bytes payload(uint32_t h) const;
    uint8_t qos(uint32_t h) const { return head(h)->flags & 3; }
    bool retain(uint32_t h) const { return head(h)->flags & 4; }
    uint32_t expiry(uint32_t h) const { return head(h)->expiry; }  // 0: never
    uint16_t refs(uint32_t h) const { return head(h)->refs; }

    // Bytes a message of these sizes takes in the pool (header included)
    static size_t footprint(size_t topicLen, size_t propsLen, size_t payloadLen);

private:
    struct Head {
        uint16_t refs;
        uint8_t flags;  // bits 0-1 QoS, bit 2 retain
        uint8_t reserved;
        uint16_t topicLen;
        uint16_t reserved2;
        uint32_t propsLen;
        uint32_t payloadLen;
        uint32_t expiry;
    };

    Head *head(uint32_t h) const { return reinterpret_cast<Head *>(pool_.ptr(h)); }
    const uint8_t *data(uint32_t h) const { return pool_.ptr(h) + sizeof(Head); }

    Pool &pool_;
};

}  // namespace tinymq
