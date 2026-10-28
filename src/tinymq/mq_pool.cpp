#include "mq_pool.h"

namespace tinymq {

// Each block starts with a 4-byte header: the block size (header included,
// a multiple of 4) with bit 0 set while in use.
static const uint32_t kUsed = 1;
static const uint32_t kHeader = 4;
static const uint32_t kMinBlock = 8;

void Pool::begin(void *mem, size_t size)
{
    uintptr_t p = reinterpret_cast<uintptr_t>(mem);
    uintptr_t aligned = (p + 3) & ~(uintptr_t)3;
    size_t skip = aligned - p;
    size = size > skip ? size - skip : 0;
    if (size > 0x7FFFFFFF)
        size = 0x7FFFFFFF;
    base_ = reinterpret_cast<uint8_t *>(aligned);
    cap_ = (uint32_t)(size & ~(size_t)3);
    used_ = 0;
    blocks_ = 0;
    if (cap_ >= kMinBlock)
        header(0) = cap_;
    else
        cap_ = 0;
}

// Merges the free block at `off` with the free blocks that follow it.
void Pool::mergeFrom(uint32_t off)
{
    uint32_t size = header(off);
    for (;;) {
        uint32_t next = off + size;
        if (next >= cap_ || (header(next) & kUsed))
            break;
        size += header(next);
    }
    header(off) = size;
}

uint32_t Pool::alloc(size_t n)
{
    if (n > cap_)
        return kNull;
    uint32_t need = (uint32_t)((n + kHeader + 3) & ~(size_t)3);
    if (need < kMinBlock)
        need = kMinBlock;
    for (uint32_t off = 0; off < cap_;) {
        uint32_t h = header(off);
        if (h & kUsed) {
            off += h & ~kUsed;
            continue;
        }
        mergeFrom(off);
        uint32_t size = header(off);
        if (size >= need) {
            if (size - need >= kMinBlock) {
                header(off + need) = size - need;
                size = need;
            }
            header(off) = size | kUsed;
            used_ += size;
            ++blocks_;
            return off + kHeader;
        }
        off += size;
    }
    return kNull;
}

void Pool::free(uint32_t h)
{
    if (h == kNull)
        return;
    uint32_t off = h - kHeader;
    uint32_t size = header(off) & ~kUsed;
    header(off) = size;
    used_ -= size;
    --blocks_;
    mergeFrom(off);
}

size_t Pool::largestFree()
{
    uint32_t best = 0;
    for (uint32_t off = 0; off < cap_;) {
        uint32_t h = header(off);
        if (!(h & kUsed)) {
            mergeFrom(off);
            h = header(off);
            if (h > best)
                best = h;
        }
        off += h & ~kUsed;
    }
    return best >= kMinBlock ? best - kHeader : 0;
}

// ---- MessageStore

size_t MessageStore::footprint(size_t topicLen, size_t propsLen, size_t payloadLen)
{
    size_t n = sizeof(Head) + topicLen + propsLen + payloadLen;
    return ((n + kHeader + 3) & ~(size_t)3);
}

uint32_t MessageStore::create(Bytes topic, Bytes props, Bytes payload, uint8_t qos, bool retain, uint32_t expiry)
{
    uint8_t *p;
    uint32_t h = createEmpty(topic, props, payload.len, qos, retain, expiry, p);
    if (h != Pool::kNull && payload.len)
        memcpy(p, payload.data, payload.len);
    return h;
}

uint32_t MessageStore::createEmpty(Bytes topic, Bytes props, size_t payloadLen, uint8_t qos, bool retain, uint32_t expiry, uint8_t *&payload)
{
    if (topic.len > 0xFFFF)
        return Pool::kNull;
    uint32_t h = pool_.alloc(sizeof(Head) + topic.len + props.len + payloadLen);
    if (h == Pool::kNull)
        return h;
    Head *m = head(h);
    m->refs = 1;
    m->flags = (uint8_t)((qos & 3) | (retain ? 4 : 0));
    m->reserved = 0;
    m->topicLen = (uint16_t)topic.len;
    m->reserved2 = 0;
    m->propsLen = props.len;
    m->payloadLen = (uint32_t)payloadLen;
    m->expiry = expiry;
    uint8_t *d = pool_.ptr(h) + sizeof(Head);
    if (topic.len)
        memcpy(d, topic.data, topic.len);
    if (props.len)
        memcpy(d + topic.len, props.data, props.len);
    payload = d + topic.len + props.len;
    return h;
}

void MessageStore::addRef(uint32_t h)
{
    ++head(h)->refs;
}

void MessageStore::release(uint32_t h)
{
    if (h == Pool::kNull)
        return;
    if (--head(h)->refs == 0)
        pool_.free(h);
}

Bytes MessageStore::topic(uint32_t h) const
{
    return Bytes(data(h), head(h)->topicLen);
}

Bytes MessageStore::props(uint32_t h) const
{
    return Bytes(data(h) + head(h)->topicLen, head(h)->propsLen);
}

Bytes MessageStore::payload(uint32_t h) const
{
    const Head *m = head(h);
    return Bytes(data(h) + m->topicLen + m->propsLen, m->payloadLen);
}

}  // namespace tinymq
