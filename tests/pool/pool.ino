// Pool unit tests: allocation, merging, exhaustion, alignment, a long
// random alloc / free run checked against a shadow table, and
// reference-counted messages.

#include <TinyMQ.h>
#include "expect.h"

using namespace tinymq;

alignas(4) static uint8_t g_arena[4096 + 3];

static void testBasics()
{
    Pool pool;
    pool.begin(g_arena + 1, 1024);  // misaligned on purpose
    EXPECT_TRUE("arena aligned and trimmed", pool.capacity() <= 1023 && pool.capacity() >= 1016 && (pool.capacity() & 3) == 0);
    size_t cap = pool.capacity();

    uint32_t a = pool.alloc(10);
    uint32_t b = pool.alloc(1);
    uint32_t c = pool.alloc(100);
    EXPECT_TRUE("three blocks", a != Pool::kNull && b != Pool::kNull && c != Pool::kNull && pool.blocks() == 3);
    EXPECT_TRUE("blocks 4-byte aligned", ((uintptr_t)pool.ptr(a) & 3) == 0 && ((uintptr_t)pool.ptr(b) & 3) == 0 && ((uintptr_t)pool.ptr(c) & 3) == 0);
    EXPECT_TRUE("blocks do not overlap", pool.ptr(b) >= pool.ptr(a) + 10 && pool.ptr(c) >= pool.ptr(b) + 1);
    EXPECT_EQ("used counts headers", pool.used(), (size_t)(16 + 8 + 104));

    pool.free(b);
    uint32_t b2 = pool.alloc(4);
    EXPECT_EQ("a freed hole is reused first-fit", b2, b);
    pool.free(b2);
    pool.free(a);
    EXPECT_TRUE("freeing the first block merges with the hole", pool.alloc(20) == a);
    pool.free(a);
    pool.free(c);
    EXPECT_EQ("all freed", pool.used(), (size_t)0);
    EXPECT_EQ("all merged back", pool.largestFree(), cap - 4);

    uint32_t whole = pool.alloc(cap - 4);
    EXPECT_TRUE("the whole arena in one block", whole != Pool::kNull);
    EXPECT_TRUE("nothing left", pool.alloc(1) == Pool::kNull);
    pool.free(whole);
    EXPECT_TRUE("too large for the arena", pool.alloc(cap) == Pool::kNull);
    EXPECT_TRUE("zero bytes is a block", pool.alloc(0) != Pool::kNull);
}

static void testFragmentation()
{
    Pool pool;
    pool.begin(g_arena, 256);
    uint32_t h[8];
    for (int i = 0; i < 8; ++i)
        h[i] = pool.alloc(28);  // 32-byte blocks fill 256 exactly
    EXPECT_TRUE("arena full", h[7] != Pool::kNull && pool.alloc(1) == Pool::kNull);
    for (int i = 0; i < 8; i += 2)
        pool.free(h[i]);
    EXPECT_EQ("holes do not merge across used blocks", pool.largestFree(), (size_t)28);
    EXPECT_TRUE("a block larger than any hole fails", pool.alloc(40) == Pool::kNull);
    pool.free(h[1]);
    EXPECT_EQ("freeing a neighbour merges three blocks", pool.largestFree(), (size_t)92);
    EXPECT_TRUE("the merged hole is usable", pool.alloc(90) == h[0]);
}

// Random alloc / free against a shadow table; every block is filled with
// its own pattern and checked before it is freed.
static void testRandom()
{
    Pool pool;
    pool.begin(g_arena, 4096);
    const int kSlots = 48;
    uint32_t handle[kSlots];
    uint16_t size[kSlots];
    for (int i = 0; i < kSlots; ++i)
        handle[i] = Pool::kNull;

    uint32_t seed = 12345;
    bool ok = true;
    int allocs = 0, failures = 0;
    for (int step = 0; step < 20000 && ok; ++step) {
        seed = seed * 1103515245 + 12345;
        int slot = (seed >> 8) % kSlots;
        if (handle[slot] == Pool::kNull) {
            uint16_t n = (uint16_t)((seed >> 16) % 300);
            uint32_t h = pool.alloc(n);
            if (h == Pool::kNull) {
                ++failures;
                continue;
            }
            ++allocs;
            handle[slot] = h;
            size[slot] = n;
            memset(pool.ptr(h), slot, n);
        } else {
            uint8_t *p = pool.ptr(handle[slot]);
            for (uint16_t k = 0; k < size[slot]; ++k)
                if (p[k] != (uint8_t)slot)
                    ok = false;
            pool.free(handle[slot]);
            handle[slot] = Pool::kNull;
        }
    }
    EXPECT_TRUE("random run keeps every block intact", ok);
    EXPECT_TRUE("random run allocated and sometimes ran out", allocs > 1000 && failures > 0);
    for (int i = 0; i < kSlots; ++i)
        pool.free(handle[i]);
    EXPECT_EQ("random run frees everything", pool.used(), (size_t)0);
    EXPECT_EQ("random run merges back to one block", pool.largestFree(), pool.capacity() - 4);
}

static void testMessages()
{
    Pool pool;
    pool.begin(g_arena, 1024);
    MessageStore store(pool);
    static const uint8_t props[] = {0x01, 0x01};
    uint32_t m = store.create(Bytes("a/b"), Bytes(props, 2), Bytes("payload"), 2, true, 1234);
    EXPECT_TRUE("message created", m != Pool::kNull);
    EXPECT_TRUE("message fields", store.topic(m).equals("a/b") && store.props(m).len == 2 && store.props(m).data[0] == 1 && store.payload(m).equals("payload") && store.qos(m) == 2 && store.retain(m) && store.expiry(m) == 1234);
    EXPECT_EQ("message footprint", pool.used(), MessageStore::footprint(3, 2, 7));

    store.addRef(m);
    store.addRef(m);
    EXPECT_EQ("three references", store.refs(m), (uint16_t)3);
    store.release(m);
    store.release(m);
    EXPECT_TRUE("still alive with one reference", pool.used() > 0 && store.payload(m).equals("payload"));
    store.release(m);
    EXPECT_EQ("freed with the last reference", pool.used(), (size_t)0);

    uint32_t empty = store.create(Bytes("t"), Bytes(), Bytes(), 0, false);
    EXPECT_TRUE("empty payload message", empty != Pool::kNull && store.payload(empty).len == 0 && !store.retain(empty));
    store.release(empty);

    static uint8_t big[2000];
    EXPECT_TRUE("message larger than the pool fails", store.create(Bytes("t"), Bytes(), Bytes(big, sizeof(big)), 0, false) == Pool::kNull);
}

void setup()
{
    Serial.begin(115200);
    delay(5000);  // give the real ESP32 Serial-USB bridge time to settle
    testBasics();
    testFragmentation();
    testRandom();
    testMessages();
    finishTests();
}

void loop()
{
    delay(1);
}
