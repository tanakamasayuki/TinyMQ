"""TinyMQBroker (MQTT 3.1.1 / 3.1 / 5.0 over TCP) against paho-mqtt and raw
sockets (5.0 in v5_scenarios.py), and
its MQTT-SN gateway against a Python MQTT-SN client (sn_scenarios.py).

The host DUT accepts one serial connection per process, so the scenarios
run inside one test function and are reported one by one
(SCENARIO PASS/FAIL and junit properties). The last scenario checks that
everything the others left behind was freed.
"""

import re
import socket
import time

import paho.mqtt.client as mqtt

from helpers import mqtt_packets as mp
from helpers import target
from helpers.line_dut import LineDut
from helpers.mosquitto import free_port
from helpers.paho_peer import PahoPeer
from sn_scenarios import SCENARIOS as SN_SCENARIOS, free_udp_port
from v5_scenarios import SCENARIOS as V5_SCENARIOS


class Dut(LineDut):
    """Serial commands of broker_interop.ino."""

    def setting(self, line):
        """Sends a settings command and waits until it took effect."""
        self.send(line)
        self.expect(rf"^OK {line.split()[0]}$")

    def stats(self):
        self.send("STATS")
        m = self.expect(r"^STATS clients=(\d+) sessions=(\d+) subs=(\d+) retained=(\d+) pool=(\d+)/(\d+) dropped=(\d+) sn=(\d+) snregs=(\d+)")
        keys = ("clients", "sessions", "subs", "retained", "pool", "capacity", "dropped", "sn", "snregs")
        return dict(zip(keys, (int(g) for g in m.groups())))

    def wait_clients(self, n, timeout=5):
        deadline = time.time() + timeout
        while True:
            s = self.stats()
            if s["clients"] == n:
                return s
            if time.time() > deadline:
                raise AssertionError(f"{s['clients']} clients connected, expected {n}")
            time.sleep(0.1)


def peer(port, client_id, **kwargs):
    return PahoPeer(port, client_id=client_id, **kwargs)


def received(pc, n, timeout=5):
    return [pc.expect(timeout) for _ in range(n)]


# ---- scenarios


def scenario_pubsub(d, port):
    a, b = peer(port, "a"), peer(port, "b")
    try:
        b.subscribe("t/#", 2)
        for qos in (0, 1, 2):
            a.publish("t/x", f"q{qos}", qos=qos)
            m = b.expect()
            assert (m.topic, m.payload, m.qos, m.retain) == ("t/x", f"q{qos}".encode(), qos, False)
        # overlapping subscriptions: one delivery at the highest QoS
        b.subscribe("t/x", 1)
        a.publish("t/x", "overlap", qos=2)
        m = b.expect()
        assert (m.payload, m.qos) == (b"overlap", 2)
        b.expect_none(0.5)
        # the publisher's QoS caps the delivery
        a.publish("t/x", "low", qos=0)
        assert b.expect().qos == 0
    finally:
        a.close()
        b.close()


def scenario_max_qos(d, port):
    d.setting("MAXQOS 1")
    a, b = peer(port, "a"), peer(port, "b")
    try:
        granted = []
        b.client.on_subscribe = lambda c, u, mid, codes, props: granted.extend(int(x.value) for x in codes)
        b.subscribe("m/#", 2)
        assert granted == [1], granted
        a.publish("m/1", "two", qos=2)  # still accepted at QoS 2
        m = b.expect()
        assert (m.payload, m.qos) == (b"two", 1)
    finally:
        d.setting("MAXQOS 2")
        a.close()
        b.close()


def scenario_wildcards(d, port):
    a, b = peer(port, "a"), peer(port, "b")
    try:
        b.subscribe("+/b", 0)
        b.subscribe("w/#", 0)
        for topic in ("w/b", "w", "x/b", "x/y/b", "w/c/d"):
            a.publish(topic, topic, qos=0)
        got = sorted(m.topic for m in received(b, 4))
        assert got == ["w", "w/b", "w/c/d", "x/b"], got
        b.expect_none(0.5)
        # "#" does not see $-topics, and clients cannot publish to $SYS
        b.subscribe("#", 0)
        a.publish("$SYS/x", "no", qos=1)
        a.publish("$foo", "no", qos=1)
        b.expect_none(0.5)
    finally:
        a.close()
        b.close()


def scenario_retain(d, port):
    a, b = peer(port, "a"), peer(port, "b")
    c = c2 = None
    try:
        a.publish("r/1", "v1", qos=1, retain=True)
        b.subscribe("r/#", 1)
        m = b.expect()
        assert (m.payload, m.retain, m.qos) == (b"v1", True, 1)
        # forwarding to an existing subscriber clears the retain flag
        a.publish("r/1", "v2", qos=1, retain=True)
        m = b.expect()
        assert (m.payload, m.retain) == (b"v2", False)
        # subscribing again with the same filter resends retained messages
        b.subscribe("r/#", 0)
        m = b.expect()
        assert (m.payload, m.retain, m.qos) == (b"v2", True, 0)
        c = peer(port, "c")
        c.subscribe("r/1", 0)
        assert c.expect().payload == b"v2"
        assert d.stats()["retained"] == 1
        a.publish("r/1", "", qos=1, retain=True)  # deletes
        b.expect()
        c2 = peer(port, "c2")
        c2.subscribe("r/#", 0)
        c2.expect_none(0.5)
        assert d.stats()["retained"] == 0
    finally:
        for p in (a, b, c, c2):
            if p:
                p.close()


def scenario_will(d, port):
    b = peer(port, "b")
    try:
        b.subscribe("w/#", 1)
        r = mp.RawClient(port)
        r.connect("willer", will=("w/1", "bye", 1, False))
        r.close()  # no DISCONNECT
        m = b.expect()
        assert (m.topic, m.payload, m.qos) == ("w/1", b"bye", 1)
        r = mp.RawClient(port)
        r.connect("willer", will=("w/1", "bye", 1, False))
        r.send(mp.DISCONNECT)
        assert r.closed_by_peer()
        b.expect_none(0.5)
        # a keep alive timeout publishes the will too
        r = mp.RawClient(port)
        r.connect("sleepy", keepalive=1, will=("w/2", "timeout", 0, False))
        t0 = time.time()
        assert r.closed_by_peer(5)
        elapsed = time.time() - t0
        assert 1.2 < elapsed < 3, elapsed
        assert b.expect().payload == b"timeout"
    finally:
        b.close()


def scenario_persistent(d, port):
    a = peer(port, "a")
    try:
        p = peer(port, "keeper", clean_session=False)
        p.subscribe("ps/#", 1)
        p.close()
        a.publish("ps/1", "one", qos=1)
        a.publish("ps/2", "zero", qos=0)  # not kept for an offline client
        a.publish("ps/3", "two", qos=2)
        r = mp.RawClient(port)
        assert r.connect("keeper", clean=False) is True
        got = []
        for _ in range(2):
            first, body = r.recv_packet()
            topic, pid, payload, qos, dup, _ = mp.parse_publish(first, body)
            got.append((topic, payload, qos))
            if qos == 1:
                r.send(mp.ack(4, pid))
            else:
                r.send(mp.ack(5, pid))
                assert r.recv_packet() == (0x62, pid.to_bytes(2, "big"))
                r.send(mp.ack(7, pid))
        assert got == [("ps/1", b"one", 1), ("ps/3", b"two", 1)], got
        r.send(mp.DISCONNECT)
        r.close()
        # a clean connection removes the session
        r = mp.RawClient(port)
        assert r.connect("keeper", clean=True) is False
        r.send(mp.DISCONNECT)
        r.close()
        r = mp.RawClient(port)
        assert r.connect("keeper", clean=False) is False
        r.send(mp.DISCONNECT)
        r.close()
        r = mp.RawClient(port)
        r.connect("keeper", clean=True)
        r.send(mp.DISCONNECT)
        r.close()
    finally:
        a.close()


def scenario_takeover(d, port):
    r1 = mp.RawClient(port)
    r1.connect("same")
    r2 = mp.RawClient(port)
    r2.connect("same")
    assert r1.closed_by_peer(), "the first connection was not closed"
    r2.send(mp.PINGREQ)
    assert r2.recv_packet() == (0xD0, b"")
    r2.send(mp.DISCONNECT)
    r2.close()
    r1.close()


def scenario_resend(d, port):
    """QoS 1 / 2 deliveries not acknowledged are resent with DUP on reconnect."""
    a = peer(port, "a")
    try:
        r = mp.RawClient(port)
        r.connect("rs", clean=False)
        assert r.subscribe(1, [("rs/#", 2)]) == [2]
        a.publish("rs/1", "one", qos=1)
        topic, pid1, payload, qos, dup, _ = mp.parse_publish(*r.recv_packet())
        assert (topic, payload, qos, dup) == ("rs/1", b"one", 1, False)
        a.publish("rs/2", "two", qos=2)
        _, pid2, _, qos, _, _ = mp.parse_publish(*r.recv_packet())
        assert qos == 2
        r.send(mp.ack(5, pid2))  # PUBREC
        assert r.recv_packet() == (0x62, pid2.to_bytes(2, "big"))  # PUBREL
        r.close()  # no PUBACK, no PUBCOMP

        r = mp.RawClient(port)
        assert r.connect("rs", clean=False) is True
        topic, pid, payload, qos, dup, _ = mp.parse_publish(*r.recv_packet())
        assert (topic, pid, payload, qos, dup) == ("rs/1", pid1, b"one", 1, True)
        assert r.recv_packet() == (0x62, pid2.to_bytes(2, "big")), "PUBREL resent"
        r.send(mp.ack(4, pid1))
        r.send(mp.ack(7, pid2))
        r.send(mp.PINGREQ)
        assert r.recv_packet() == (0xD0, b"")
        r.send(mp.DISCONNECT)
        r.close()
        r = mp.RawClient(port)
        assert r.connect("rs", clean=True) is False
        r.send(mp.DISCONNECT)
        r.close()
    finally:
        a.close()


def scenario_qos2_in(d, port):
    b = peer(port, "b")
    try:
        b.subscribe("q2/#", 2)
        r = mp.RawClient(port)
        r.connect("q2pub")
        r.send(mp.publish("q2/x", "once", qos=2, packet_id=9))
        assert r.recv_packet() == (0x50, b"\x00\x09")
        r.send(mp.publish("q2/x", "once", qos=2, packet_id=9, dup=True))
        assert r.recv_packet() == (0x50, b"\x00\x09")
        r.send(mp.ack(6, 9))
        assert r.recv_packet() == (0x70, b"\x00\x09")
        r.send(mp.publish("q2/x", "again", qos=2, packet_id=9))
        assert r.recv_packet() == (0x50, b"\x00\x09")
        r.send(mp.ack(6, 9))
        r.recv_packet()
        assert [m.payload for m in received(b, 2)] == [b"once", b"again"]
        b.expect_none(0.5)
        r.send(mp.DISCONNECT)
        r.close()
    finally:
        b.close()


def scenario_auth(d, port):
    d.setting("AUTH 1")
    try:
        for kwargs, code in (({}, 5), ({"username": "bob", "password": "secret"}, 5), ({"username": "alice", "password": "nope"}, 4), ({"username": "alice"}, 4)):
            r = mp.RawClient(port)
            r.connect("authc", expect_code=code, **kwargs)
            assert r.closed_by_peer()
            r.close()
        r = mp.RawClient(port)
        r.connect("authc", username="alice", password="secret")
        r.send(mp.DISCONNECT)
        r.close()
    finally:
        d.setting("AUTH 0")


def scenario_acl(d, port):
    d.setting("ACL 1")
    a, b = peer(port, "a"), peer(port, "b")
    try:
        r = mp.RawClient(port)
        r.connect("aclc")
        assert r.subscribe(3, [("secret/#", 1), ("open/#", 1)]) == [0x80, 1]
        r.send(mp.DISCONNECT)
        r.close()
        b.subscribe("#", 0)
        a.publish("secret/x", "hidden", qos=1)  # acknowledged, not delivered
        a.publish("open/x", "shown", qos=1)
        assert b.expect().payload == b"shown"
        b.expect_none(0.5)
    finally:
        d.setting("ACL 0")
        a.close()
        b.close()


def scenario_local(d, port):
    a = peer(port, "a")
    try:
        a.publish("loc/r", "kept", qos=1, retain=True)
        d.send("LSUB 1 loc/#")
        d.expect(r"^LSUB ok")
        m = d.expect(r"^LOCAL (\d) (\d) (\S+) (.*)$")
        assert m.groups() == ("1", "1", "loc/r", "kept")
        a.publish("loc/a", "hello", qos=2)
        m = d.expect(r"^LOCAL (\d) (\d) (\S+) (.*)$")
        assert m.groups() == ("1", "0", "loc/a", "hello")
        a.subscribe("out/#", 1)
        d.send("LPUB 1 0 out/x from-sketch")
        d.expect(r"^LPUB ok")
        m = a.expect()
        assert (m.topic, m.payload, m.qos) == ("out/x", b"from-sketch", 1)
        d.send("LUNSUB loc/#")
        d.expect(r"^LUNSUB ok")
        a.publish("loc/a", "unheard", qos=0)
        d.expect_none(r"^LOCAL", 0.5)
        a.publish("loc/r", "", qos=1, retain=True)
    finally:
        a.close()


def scenario_unsubscribe(d, port):
    a, b = peer(port, "a"), peer(port, "b")
    try:
        b.subscribe("u/#", 0)
        a.publish("u/1", "x", qos=0)
        b.expect()
        b.client.unsubscribe("u/#")
        time.sleep(0.2)
        a.publish("u/1", "y", qos=0)
        b.expect_none(0.5)
    finally:
        a.close()
        b.close()


def scenario_protocol_errors(d, port):
    # a PUBLISH before CONNECT
    r = mp.RawClient(port)
    r.send(mp.publish("x", "y"))
    assert r.closed_by_peer()
    # an unknown protocol level gets return code 1
    r = mp.RawClient(port)
    r.send(mp.connect("lvl", level=6))
    assert r.recv_packet() == (0x20, b"\x00\x01")
    assert r.closed_by_peer()
    # MQTT 5.0 is accepted (its CONNACK has a reason code and properties)
    r = mp.RawClient(port)
    # level 5, clean start, keep alive 60, no properties
    r.send(mp.packet(0x10, mp.string("MQTT") + bytes([5, 0x02, 0, 60, 0]) + mp.string("v5")))
    first, body = r.recv_packet()
    assert first == 0x20 and body[:2] == b"\x00\x00", body
    r.send(mp.DISCONNECT)
    # an empty client id needs a clean session
    r = mp.RawClient(port)
    r.connect("", clean=False, expect_code=2)
    r = mp.RawClient(port)
    r.connect("", clean=True)
    r.send(mp.DISCONNECT)
    # a second CONNECT
    r = mp.RawClient(port)
    r.connect("twice")
    r.send(mp.connect("twice"))
    assert r.closed_by_peer()
    # reserved flags in the fixed header
    r = mp.RawClient(port)
    r.connect("flags")
    r.send(bytes([0x80, 0x06]) + b"\x00\x01\x00\x01a\x00")  # SUBSCRIBE without 0010
    assert r.closed_by_peer()
    # a packet larger than the receive buffer
    r = mp.RawClient(port)
    r.connect("big")
    r.send(mp.publish("big", b"x" * 2000))
    assert r.closed_by_peer()
    # no CONNECT in time
    d.setting("CTIMEOUT 500")
    r = mp.RawClient(port)
    t0 = time.time()
    assert r.closed_by_peer(5)
    assert time.time() - t0 < 2
    d.setting("CTIMEOUT 10000")
    d.wait_clients(0)


def scenario_mqtt31(d, port):
    a = peer(port, "old-a", protocol=mqtt.MQTTv31)
    b = peer(port, "old-b", protocol=mqtt.MQTTv31)
    try:
        b.subscribe("v31/#", 2)
        a.publish("v31/x", "hello", qos=2)
        assert b.expect().payload == b"hello"
    finally:
        a.close()
        b.close()


def scenario_many_clients(d, port):
    """All eight connections in use; a ninth is closed at once."""
    peers = [peer(port, f"m{i}") for i in range(8)]
    try:
        for p in peers:
            p.subscribe("all", 1)
        d.wait_clients(8)
        r = mp.RawClient(port)
        assert r.closed_by_peer(3)
        peers[0].publish("all", "fan-out", qos=1)
        for p in peers:
            assert p.expect().payload == b"fan-out"
    finally:
        for p in peers:
            p.close()
    d.wait_clients(0)


def scenario_payload_sizes(d, port):
    a, b = peer(port, "a"), peer(port, "b")
    try:
        b.subscribe("sz", 1)
        for n in (0, 1, 127, 128, 900):
            a.publish("sz", b"p" * n, qos=1)
            assert b.expect().payload == b"p" * n
    finally:
        a.close()
        b.close()


def scenario_clean_state(d, port):
    """Everything the scenarios left was freed: no clients, sessions, subscriptions or pool bytes."""
    d.wait_clients(0)
    s = d.stats()
    assert (s["sessions"], s["subs"], s["retained"], s["pool"], s["sn"], s["snregs"]) == (0, 0, 0, 0, 0, 0), s


def test_broker_interop(dut, record_property):
    d = Dut(dut)
    ip = d.ready()
    if target.is_local(ip):
        port, sn_port = free_port(), free_udp_port()
    else:
        port, sn_port = 1883, 1884  # a board: the peers connect to its address
        target.HOST = ip
    d.send(f"LISTEN {port} {sn_port}")
    d.expect(rf"^LISTENING {port}")
    results = {}

    def run(name, fn):
        try:
            fn(d, port)
            results[name] = "PASS"
        except Exception as e:  # report every scenario, then fail at the end
            results[name] = f"FAIL {type(e).__name__}: {e}"
        print(f"SCENARIO {results[name].split()[0]} {name} {results[name][4:]}")
        record_property(f"scenario_{name}", results[name])

    for name, fn in (
        ("pubsub", scenario_pubsub),
        ("max_qos", scenario_max_qos),
        ("wildcards", scenario_wildcards),
        ("retain", scenario_retain),
        ("will", scenario_will),
        ("persistent", scenario_persistent),
        ("takeover", scenario_takeover),
        ("resend", scenario_resend),
        ("qos2_in", scenario_qos2_in),
        ("auth", scenario_auth),
        ("acl", scenario_acl),
        ("local", scenario_local),
        ("unsubscribe", scenario_unsubscribe),
        ("protocol_errors", scenario_protocol_errors),
        ("mqtt31", scenario_mqtt31),
        ("many_clients", scenario_many_clients),
        ("payload_sizes", scenario_payload_sizes),
    ):
        run(name, fn)
    for name, fn in V5_SCENARIOS:
        run(name, fn)
    for name, fn in SN_SCENARIOS:
        run(name, lambda d, port, fn=fn: fn(d, port, sn_port))
    run("clean_state", scenario_clean_state)
    target.HOST = "127.0.0.1"

    failed = {k: v for k, v in results.items() if v != "PASS"}
    assert not failed, failed
