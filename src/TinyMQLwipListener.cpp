#include "TinyMQLwipListener.h"

#ifdef TINYMQ_HAS_LWIP

#if defined(ARDUINO_ARCH_RP2040)
#include <lwip_wrap.h>
#include <new>
#elif !NO_SYS && LWIP_TCPIP_CORE_LOCKING
#include <lwip/tcpip.h>
#endif

namespace {

// ESP32: the lwIP core lock, which does not nest, so taken once however
// deep the calls go (only loop()'s task takes it here). arduino-pico: the
// recursive lock its lwIP wrappers take (LWIPMutex has no state, so
// constructing and destroying one is the lock and the unlock). CH32V: lwIP
// runs only from yield(), no lock.
#if defined(ARDUINO_ARCH_RP2040)
uint8_t lwipMutexStorage[sizeof(LWIPMutex)];
#else
int lockDepth = 0;
#endif

struct LwipLock {
    LwipLock()
    {
#if defined(ARDUINO_ARCH_RP2040)
        new (lwipMutexStorage) LWIPMutex();
#elif !NO_SYS && LWIP_TCPIP_CORE_LOCKING
        if (lockDepth++ == 0)
            LOCK_TCPIP_CORE();
#endif
    }
    ~LwipLock()
    {
#if defined(ARDUINO_ARCH_RP2040)
        reinterpret_cast<LWIPMutex *>(lwipMutexStorage)->~LWIPMutex();
#elif !NO_SYS && LWIP_TCPIP_CORE_LOCKING
        if (--lockDepth == 0)
            UNLOCK_TCPIP_CORE();
#endif
    }
};

// Room kept in the send buffer for acknowledgements and other small packets
const size_t kReserve = 64;
const u16_t kReserveSegments = 4;

}  // namespace

bool TinyMQLwipListener::begin()
{
    LwipLock lock;
    if (listen_)
        return true;
    for (Slot &s : slots_)
        s.owner = this;
    tcp_pcb *pcb = tcp_new_ip_type(IPADDR_TYPE_ANY);
    if (!pcb)
        return false;
    if (tcp_bind(pcb, IP_ANY_TYPE, port_) != ERR_OK) {
        tcp_close(pcb);
        return false;
    }
    listen_ = tcp_listen(pcb);  // frees pcb, or keeps it on failure
    if (!listen_) {
        tcp_close(pcb);
        return false;
    }
    tcp_arg(listen_, this);
    tcp_accept(listen_, onAccept);
    return true;
}

void TinyMQLwipListener::end()
{
    LwipLock lock;
    for (Slot &s : slots_)
        if (s.pcb) {
            tcp_abort(s.pcb);  // calls onError, which forgets it
            s.release();
        }
    if (listen_) {
        tcp_close(listen_);
        listen_ = nullptr;
    }
}

err_t TinyMQLwipListener::onAccept(void *arg, tcp_pcb *pcb, err_t err)
{
    TinyMQLwipListener *self = static_cast<TinyMQLwipListener *>(arg);
    if (err != ERR_OK || !pcb)
        return ERR_VAL;
    for (Slot &s : self->slots_) {
        if (s.pcb || s.fresh || s.conn >= 0 || s.gone)
            continue;
        s.pcb = pcb;
        s.fresh = true;
        s.rx = nullptr;
        tcp_arg(pcb, &s);
        tcp_recv(pcb, onRecv);
        tcp_err(pcb, onError);
        tcp_nagle_disable(pcb);  // MQTT packets are small and want to go now
        return ERR_OK;
    }
    tcp_abort(pcb);
    return ERR_ABRT;
}

err_t TinyMQLwipListener::onRecv(void *arg, tcp_pcb *pcb, pbuf *p, err_t err)
{
    (void)pcb;
    Slot *s = static_cast<Slot *>(arg);
    if (!s) {
        if (p)
            pbuf_free(p);
        return ERR_OK;
    }
    if (!p || err != ERR_OK) {
        if (p)
            pbuf_free(p);
        s->gone = true;  // the peer closed
        return ERR_OK;
    }
    if (s->rx)
        pbuf_cat(s->rx, p);
    else
        s->rx = p;
    return ERR_OK;
}

void TinyMQLwipListener::onError(void *arg, err_t err)
{
    (void)err;
    Slot *s = static_cast<Slot *>(arg);
    if (!s)
        return;
    s->pcb = nullptr;  // already freed by lwIP
    s->gone = true;
}

void TinyMQLwipListener::update(tinymq::Broker &engine, uint32_t now)
{
    LwipLock lock;
    for (Slot &s : slots_) {
        if (s.fresh) {
            s.fresh = false;
            s.conn = s.pcb ? engine.accept(s, now) : -1;
            if (s.conn < 0) {
                s.close();
                continue;
            }
        }
        if (s.conn < 0) {
            if (s.gone && !s.pcb)
                s.release();
            continue;
        }
        if (!engine.owns(s.conn, s)) {
            s.conn = -1;  // the broker closed it (close() released it)
            continue;
        }
        while (s.rx && engine.owns(s.conn, s)) {
            size_t room;
            uint8_t *buf = engine.rxBuffer(s.conn, room);
            if (room == 0)
                break;
            size_t n = s.rx->tot_len < room ? s.rx->tot_len : room;
            pbuf_copy_partial(s.rx, buf, (u16_t)n, 0);
            s.rx = pbuf_free_header(s.rx, (u16_t)n);
            if (s.pcb) {
                tcp_recved(s.pcb, (u16_t)n);
                // acknowledge now: a client with Nagle's algorithm on holds its
                // next small packet until this ACK, which lwIP would delay up
                // to 250 ms when there is no answer to carry it (QoS 0)
                tcp_set_flags(s.pcb, TF_ACK_NOW);
                tcp_output(s.pcb);
            }
            engine.rxCommit(s.conn, n, now);
        }
        if (engine.owns(s.conn, s) && s.gone && !s.rx)
            engine.linkLost(s.conn);  // close() follows
        if (!engine.owns(s.conn, s)) {
            s.conn = -1;
            if (s.gone)
                s.release();
        }
    }
    // what the broker wrote, from this listener or any other, goes out now
    for (Slot &s : slots_)
        if (s.unsent && s.pcb) {
            s.unsent = false;
            tcp_output(s.pcb);
        }
}

size_t TinyMQLwipListener::Slot::write(const uint8_t *data, size_t len)
{
    LwipLock lock;
    if (!pcb || gone)
        return 0;
    size_t done = 0;
    while (done < len) {
        size_t room = tcp_sndbuf(pcb);
        if (room == 0)
            break;
        size_t n = len - done < room ? len - done : room;
        if (n > 0xFFFF)
            n = 0xFFFF;
        if (tcp_write(pcb, data + done, (u16_t)n, TCP_WRITE_FLAG_COPY) != ERR_OK)
            break;
        done += n;
    }
    // sent at the end of update(): small packets written meanwhile share segments
    unsent = unsent || done > 0;
    return done;
}

size_t TinyMQLwipListener::Slot::writable()
{
    LwipLock lock;
    if (!pcb || gone)
        return 0;
    // both limits of the send queue: bytes, and the number of segments
    // (TCP_SND_QUEUELEN), which runs out first with many small packets and
    // would make tcp_write() fail half way
    if (tcp_sndqueuelen(pcb) + kReserveSegments >= TCP_SND_QUEUELEN)
        return 0;
    size_t room = tcp_sndbuf(pcb);
    return room > kReserve ? room - kReserve : 0;
}

void TinyMQLwipListener::Slot::close()
{
    LwipLock lock;
    if (pcb) {
        tcp_arg(pcb, nullptr);
        tcp_recv(pcb, nullptr);
        tcp_err(pcb, nullptr);
        // what the broker said last (a DISCONNECT) goes first; data left
        // unread is taken as read, or tcp_close() would answer with a reset
        // and drop the unsent data
        if (rx)
            tcp_recved(pcb, rx->tot_len);
        tcp_output(pcb);
        if (tcp_close(pcb) != ERR_OK)
            tcp_abort(pcb);
    }
    release();
}

void TinyMQLwipListener::Slot::release()
{
    if (rx)
        pbuf_free(rx);
    rx = nullptr;
    pcb = nullptr;
    conn = -1;
    fresh = false;
    gone = false;
    unsent = false;
}

#endif  // TINYMQ_HAS_LWIP
