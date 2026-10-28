"""MQTT 5.0 TinyMQClient scenarios, run by test_client_interop.py on the same DUT.

Each takes (d, port): the serial helper and mosquitto's port (unused by the
scenarios against the scripted broker).
"""

import time

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from helpers import fake_broker as fb
from helpers import mqtt_packets as mp
from helpers.paho_peer import PahoPeer


def v5peer(port, client_id="pc5"):
    return PahoPeer(port, client_id=client_id, protocol=mqtt.MQTTv5, host="127.0.0.1")


def server_line(d):
    m = d.expect(r"^SERVER assigned=(\S*) ka=(\d+) rmax=(\d+) mqos=(\d+) retain=(\d) maxpkt=(\d+)")
    keys = ("assigned", "ka", "rmax", "mqos", "retain", "maxpkt")
    vals = m.groups()
    return {k: (v if k == "assigned" else int(v)) for k, v in zip(keys, vals)}


def props_line(d):
    m = d.expect(r"^PROPS ct=(\S*) rt=(\S*) cd=(\S*) pfi=(-?\d+) exp=(-?\d+) sid=(\S*) up=(.*)$")
    return dict(zip(("ct", "rt", "cd", "pfi", "exp", "sid", "up"), m.groups()))


def connect5(d, port, client_id="tm5", clean=True, sexp=0, keepalive=60):
    d.reset(port, client_id=client_id, clean=clean, keepalive=keepalive, proto=5)
    d.setting(f"SEXP {sexp}")
    d.setting("WDELAY 0")
    d.setting("PROPCLEAR")
    reason, present = d.connect()
    return present, server_line(d)


def scenario_v5_props(d, port):
    pc = v5peer(port)
    try:
        pc.subscribe("c5/#", 1)
        connect5(d, port)
        d.setting("PROP CT text/plain")
        d.setting("PROP RT c5/reply")
        d.setting("PROP CD corr-1")
        d.setting("PROP UP k1 v1")
        d.setting("PROP PFI 1")
        d.setting("PROP EXP 60")
        d.send("PUBP 1 0 c5/out hello")
        pid = int(d.expect(r"^PUB (-?\d+)").group(1))
        assert pid > 0
        d.expect(rf"^PUBDONE {pid} 00")
        m = pc.expect()
        p = m.properties
        assert (m.payload, p.ContentType, p.ResponseTopic, p.CorrelationData, p.PayloadFormatIndicator) == (b"hello", "text/plain", "c5/reply", b"corr-1", 1), p
        assert p.UserProperty == [("k1", "v1")] and 55 <= p.MessageExpiryInterval <= 60, p

        d.send("SUBO 1 0 0 0 42 c5in/#")
        pid = int(d.expect(r"^SUB (\d+)").group(1))
        d.expect(rf"^SUBACK {pid} 01")
        props = Properties(PacketTypes.PUBLISH)
        props.ContentType = "application/json"
        props.UserProperty = [("a", "1"), ("b", "2")]
        pc.publish("c5in/x", "{}", qos=1, properties=props)
        msg = d.message()
        assert msg["payload"] == "{}"
        pl = props_line(d)
        assert (pl["ct"], pl["sid"], pl["up"]) == ("application/json", "42", "a=1;b=2"), pl
        d.disconnect()
    finally:
        pc.close()


def scenario_v5_no_local(d, port):
    connect5(d, port)
    d.send("SUBO 0 1 0 0 0 c5nl/a")
    d.expect(r"^SUBACK \d+ 00")
    d.send("SUBO 0 0 0 0 0 c5nl/b")
    d.expect(r"^SUBACK \d+ 00")
    d.publish(0, "c5nl/a", "own-a")
    d.publish(0, "c5nl/b", "own-b")
    m = d.message()
    assert m["topic"] == "c5nl/b", m
    d.no_message(0.5)
    d.disconnect()


def scenario_v5_session(d, port):
    pc = v5peer(port)
    try:
        present, srv = connect5(d, port, client_id="tm5s", clean=True, sexp=60)
        assert not present
        assert d.subscribe("c5s/x", 1) == [1]
        d.disconnect()
        pc.publish("c5s/x", "while-away", qos=1)
        d.send("CLEAN 0")
        reason, present = d.connect()
        server_line(d)
        assert present
        assert d.message()["payload"] == "while-away"
        d.disconnect()
        # session expiry 0: nothing kept
        present, _ = connect5(d, port, client_id="tm5s", clean=True, sexp=0)
        d.disconnect()
        d.send("CLEAN 0")
        reason, present = d.connect()
        server_line(d)
        assert not present
        d.disconnect()
    finally:
        pc.close()


def scenario_v5_assigned_and_reasons(d, port):
    d.reset(port, client_id="", proto=5)
    d.setting("SEXP 0")
    d.connect()
    srv = server_line(d)
    assert srv["assigned"], srv  # mosquitto assigned an id
    pid = d.publish(1, "c5/nobody", "x")
    d.expect(rf"^PUBDONE {pid} 10")  # no matching subscribers
    d.disconnect()


def scenario_v5_will(d, port):
    pc = v5peer(port)
    try:
        pc.subscribe("c5w/#", 1)
        d.reset(port, client_id="tm5w", proto=5)
        d.setting("SEXP 30")
        d.setting("WDELAY 0")
        d.send("WILL 1 0 c5w/1 asked")
        d.connect()
        server_line(d)
        d.send("DISC 04")  # disconnect, publishing the will
        d.expect(r"^CLOSED 00")
        assert pc.expect().payload == b"asked"
        # a will delay: published after it, not at once
        d.send("WILL 1 0 c5w/2 delayed")
        d.setting("WDELAY 2")
        d.connect()
        server_line(d)
        d.send("DROP")
        d.expect(r"^CLOSED")
        pc.expect_none(1.0)
        assert pc.expect(timeout=4).payload == b"delayed"
    finally:
        d.setting("WDELAY 0")
        d.send("NOWILL")
        pc.close()


def scenario_v5_takeover(d, port):
    connect5(d, port, client_id="tm5t")
    pc = v5peer(port, client_id="tm5t")
    try:
        d.expect(r"^CLOSED 8E")  # session taken over
    finally:
        pc.close()


def scenario_v5_server_limits(d, port):
    """CONNACK properties from a scripted server, and the client's own CONNECT properties."""
    fake = fb.FakeBroker(d.bind)
    try:
        d.reset(fake.port, client_id="tm5f", proto=5, keepalive=60)
        d.setting("SEXP 0")
        d.setting("PROPCLEAR")
        d.send("CONNECT")
        fake.accept()
        first, body = fake.recv_packet()
        assert first == 0x10 and body[6] == 5, body
        props, _ = mp.parse_props(body, 10)
        assert mp.prop(props, 0x21) == 8 and mp.prop(props, 0x27) == 1024, props  # Receive Maximum, Maximum Packet Size
        connack_props = (mp.prop_u16(0x21, 1) + mp.prop_u8(0x24, 1) + mp.prop_u8(0x25, 0) + mp.prop_u16(0x13, 1)
                         + mp.prop_u32(0x27, 60) + mp.prop_str(0x12, "fake-1"))
        fake.send(mp.packet(0x20, b"\x00\x00" + mp.props_block(connack_props)))
        d.expect(r"^CONNECT ok")
        srv = server_line(d)
        assert srv == {"assigned": "fake-1", "ka": 1, "rmax": 1, "mqos": 1, "retain": 0, "maxpkt": 60}, srv
        d.send("PUB 2 0 f/x q2")
        assert d.expect(r"^PUB (-?\d+)").group(1) == "-1", "Maximum QoS 1"
        d.send("PUB 1 1 f/x retained")
        assert d.expect(r"^PUB (-?\d+)").group(1) == "-1", "Retain Available 0"
        d.send("PUB 1 0 f/x " + "y" * 60)
        assert d.expect(r"^PUB (-?\d+)").group(1) == "-1", "Maximum Packet Size 60"
        d.send("PUB 1 0 f/x one")
        pid = int(d.expect(r"^PUB (-?\d+)").group(1))
        assert pid > 0
        first, body = fake.recv_packet()
        assert first == 0x32
        d.send("PUB 1 0 f/x two")
        assert d.expect(r"^PUB (-?\d+)").group(1) == "-1", "Receive Maximum 1"
        fake.send(mp.ack5(4, pid))
        d.expect(rf"^PUBDONE {pid} 00")
        # Server Keep Alive 1: PINGREQ within about a second
        t0 = time.time()
        assert fake.recv_packet() == (0xC0, b"")
        assert time.time() - t0 < 2.5
        fake.send(bytes([0xD0, 0x00]))
        # a topic alias we never allowed
        fake.send(mp.publish5("f/in", "x", props=mp.prop_u16(0x23, 1)))
        assert fake.recv_packet() == (0xE0, b"\x94")
        d.expect(r"^CLOSED 94")
    finally:
        fake.close()


def scenario_v5_server_disconnect(d, port):
    fake = fb.FakeBroker(d.bind)
    try:
        d.reset(fake.port, client_id="tm5d", proto=5)
        d.send("CONNECT")
        fake.accept()
        fake.recv_packet()
        fake.send(mp.packet(0x20, b"\x00\x00\x00"))
        d.expect(r"^CONNECT ok")
        fake.send(mp.disconnect5(0x8B))  # server shutting down
        d.expect(r"^CLOSED 8B")
        # a refused CONNECT with a 5.0 reason code
        d.send("CONNECT")
        fake.accept()
        fake.recv_packet()
        fake.send(mp.packet(0x20, b"\x00\x87\x00"))
        assert d.expect(r"^CONNECT (ok|fail) ([0-9A-F]{2})").groups() == ("fail", "87")
    finally:
        fake.close()


SCENARIOS = (
    ("v5_props", scenario_v5_props),
    ("v5_no_local", scenario_v5_no_local),
    ("v5_session", scenario_v5_session),
    ("v5_assigned_and_reasons", scenario_v5_assigned_and_reasons),
    ("v5_will", scenario_v5_will),
    ("v5_takeover", scenario_v5_takeover),
    ("v5_server_limits", scenario_v5_server_limits),
    ("v5_server_disconnect", scenario_v5_server_disconnect),
)
