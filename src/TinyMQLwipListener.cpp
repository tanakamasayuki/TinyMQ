#include "TinyMQLwipListener.h"

#ifdef TINYMQ_HAS_LWIP

#if defined(ARDUINO_ARCH_RP2040)
#include <lwip_wrap.h>
#include <new>
#elif !NO_SYS && LWIP_TCPIP_CORE_LOCKING
#include <lwip/tcpip.h>
#elif !NO_SYS
#include <lwip/priv/tcpip_priv.h>
#endif

namespace {

// Runs f where it may call lwIP's raw API. ESP32 with the core lock (ESP32,
// S3): under the lock, which does not nest, so taken once however deep the
// calls go. ESP32 without it (P4): in lwIP's own thread, waiting for it.
// arduino-pico: under the recursive lock its lwIP wrappers take (LWIPMutex
// has no state, so constructing and destroying one is the lock and the
// unlock). CH32V: lwIP runs only from yield(), so directly.
//
// The broker never runs inside f: its callbacks may use sockets, which
// would deadlock in lwIP's thread.
#if defined(ARDUINO_ARCH_RP2040)
uint8_t lwipMutexStorage[sizeof(LWIPMutex)];

template <class F>
void inLwip(F &&f)
{
    new (lwipMutexStorage) LWIPMutex();
    f();
    reinterpret_cast<LWIPMutex *>(lwipMutexStorage)->~LWIPMutex();
}
#elif !NO_SYS && LWIP_TCPIP_CORE_LOCKING
int lockDepth = 0;

template <class F>
void inLwip(F &&f)
{
    if (lockDepth++ == 0)
        LOCK_TCPIP_CORE();
    f();
    if (--lockDepth == 0)
        UNLOCK_TCPIP_CORE();
}
#elif !NO_SYS
template <class F>
void inLwip(F &&f)
{
    struct Call : tcpip_api_call_data {
        F *f;
    };
    Call call;
    call.f = &f;
    tcpip_api_call(
        [](tcpip_api_call_data *data) -> err_t {
            (*static_cast<Call *>(data)->f)();
            return ERR_OK;
        },
        &call);
}
#else
template <class F>
void inLwip(F &&f)
{
    f();
}
#endif

// Room kept in the send buffer for acknowledgements and other small packets
const size_t kReserve = 64;
const u16_t kReserveSegments = 4;

}  // namespace

bool TinyMQLwipListener::begin()
{
    if (listen_)
        return true;
    for (Slot &s : slots_)
        s.owner = this;
    inLwip([this] {
        tcp_pcb *pcb = tcp_new_ip_type(IPADDR_TYPE_ANY);
        if (!pcb)
            return;
        if (tcp_bind(pcb, IP_ANY_TYPE, port_) != ERR_OK) {
            tcp_close(pcb);
            return;
        }
        listen_ = tcp_listen(pcb);  // frees pcb, or keeps it on failure
        if (!listen_) {
            tcp_close(pcb);
            return;
        }
        tcp_arg(listen_, this);
        tcp_accept(listen_, onAccept);
    });
    return listen_ != nullptr;
}

void TinyMQLwipListener::end()
{
    inLwip([this] {
        for (Slot &s : slots_)
            if (s.pcb) {
                tcp_abort(s.pcb);  // calls onError, which forgets it
                s.release();
            }
        if (listen_) {
            tcp_close(listen_);
            listen_ = nullptr;
        }
    });
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
    for (Slot &s : slots_) {
        bool fresh = false, open = false;
        inLwip([&] {
            fresh = s.fresh;
            s.fresh = false;
            open = s.pcb != nullptr;
            if (s.conn < 0 && !fresh && s.gone && !s.pcb)
                s.release();
        });
        if (fresh) {
            s.conn = open ? engine.accept(s, now) : -1;
            if (s.conn < 0) {
                s.close();
                continue;
            }
        }
        if (s.conn < 0)
            continue;
        if (!engine.owns(s.conn, s)) {
            s.conn = -1;  // the broker closed it (close() released it)
            continue;
        }
        bool pending = true, gone = false;
        while (pending && engine.owns(s.conn, s)) {
            size_t room;
            uint8_t *buf = engine.rxBuffer(s.conn, room);
            if (room == 0)
                break;
            size_t n = 0;
            inLwip([&] {
                gone = s.gone;
                if (!s.rx) {
                    pending = false;
                    return;
                }
                n = s.rx->tot_len < room ? s.rx->tot_len : room;
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
            });
            if (n)
                engine.rxCommit(s.conn, n, now);
        }
        if (pending)  // stopped by a full buffer or the broker: check again
            inLwip([&] {
                gone = s.gone;
                pending = s.rx != nullptr;
            });
        if (engine.owns(s.conn, s) && gone && !pending)
            engine.linkLost(s.conn);  // close() follows
        if (!engine.owns(s.conn, s)) {
            s.conn = -1;
            if (gone)
                inLwip([&] { s.release(); });
        }
    }
    // what the broker wrote, from this listener or any other, goes out now
    inLwip([this] {
        for (Slot &s : slots_)
            if (s.unsent && s.pcb) {
                s.unsent = false;
                tcp_output(s.pcb);
            }
    });
}

size_t TinyMQLwipListener::Slot::write(const uint8_t *data, size_t len)
{
    size_t done = 0;
    inLwip([&] {
        if (!pcb || gone)
            return;
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
    });
    // sent at the end of update(): small packets written meanwhile share segments
    unsent = unsent || done > 0;
    return done;
}

size_t TinyMQLwipListener::Slot::writable()
{
    size_t room = 0;
    inLwip([&] {
        if (!pcb || gone)
            return;
        // both limits of the send queue: bytes, and the number of segments
        // (TCP_SND_QUEUELEN), which runs out first with many small packets and
        // would make tcp_write() fail half way
        if (tcp_sndqueuelen(pcb) + kReserveSegments >= TCP_SND_QUEUELEN)
            return;
        size_t buf = tcp_sndbuf(pcb);
        room = buf > kReserve ? buf - kReserve : 0;
    });
    return room;
}

void TinyMQLwipListener::Slot::close()
{
    inLwip([this] {
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
    });
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
