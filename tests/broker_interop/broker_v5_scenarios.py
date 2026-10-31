"""MQTT 5.0 broker scenarios, run by test_broker_interop.py against the same DUT.

Each takes (d, port): the serial helper and the broker's TCP port. Raw 5.0
clients check exact packets; paho-mqtt (MQTTv5) checks properties end to end.
"""

import time

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions

from helpers import mqtt_packets as mp
from helpers.paho_peer import PahoPeer


def v5(port, client_id, **kwargs):
    return PahoPeer(port, client_id=client_id, protocol=mqtt.MQTTv5, **kwargs)


def raw(port):
    return mp.RawClient5(port)


def recv_pub(r, timeout=5):
    first, body = r.recv_packet(timeout)
    assert first >> 4 == 3, (first, body)
    return mp.parse_publish5(first, body)


def no_packet(r, wait=0.5):
    try:
        first, body = r.recv_packet(wait)
    except Exception:
        return
    raise AssertionError(f"unexpected packet 0x{first:02x} {body!r}")


def scenario_v5_connack(d, port):
    r = raw(port)
    present, props = r.connect("v5c")
    assert not present
    assert mp.prop(props, mp_RECEIVE_MAX) == d.qos2_in
    assert mp.prop(props, mp_MAX_PACKET) == 1024
    assert mp.prop(props, mp_ALIAS_MAX) == 8
    assert mp.prop(props, mp_ASSIGNED) is None
    r.send(mp.disconnect5())
    assert r.closed_by_peer()
    r = raw(port)
    _, props = r.connect("")
    assigned = mp.prop(props, mp_ASSIGNED)
    assert assigned and assigned.startswith(b"tinymq-"), props
    r.send(mp.disconnect5())
    # an authentication method: no enhanced authentication here
    r = raw(port)
    r.connect("auth", expect_reason=0x8C, props=mp.prop_str(0x15, "SCRAM-SHA-1"))
    assert r.closed_by_peer()
    # a property CONNECT may not carry
    r = raw(port)
    r.connect("bad", expect_reason=0x82, props=mp.prop_u16(0x23, 1))
    assert r.closed_by_peer()


mp_RECEIVE_MAX, mp_MAX_PACKET, mp_ALIAS_MAX, mp_ASSIGNED = 0x21, 0x27, 0x22, 0x12


def scenario_v5_properties(d, port):
    a, b = v5(port, "a5"), v5(port, "b5")
    c = PahoPeer(port, client_id="c311")
    try:
        b.subscribe("pp5/#", 1)
        c.subscribe("pp5/#", 1)
        props = Properties(PacketTypes.PUBLISH)
        props.PayloadFormatIndicator = 1
        props.ContentType = "text/plain"
        props.ResponseTopic = "pp5/reply"
        props.CorrelationData = b"\x01\x02"
        props.UserProperty = [("k1", "v1"), ("k2", "v2")]
        a.publish("pp5/x", "hello", qos=1, properties=props)
        m = b.expect()
        p = m.properties
        assert (m.payload, p.PayloadFormatIndicator, p.ContentType, p.ResponseTopic, p.CorrelationData) == (b"hello", 1, "text/plain", "pp5/reply", b"\x01\x02"), p
        assert p.UserProperty == [("k1", "v1"), ("k2", "v2")], p.UserProperty
        assert c.expect().payload == b"hello"  # a 3.1.1 subscriber gets it without properties
    finally:
        a.close()
        b.close()
        c.close()


def scenario_v5_subscription_ids(d, port):
    a = PahoPeer(port, client_id="a")
    r = raw(port)
    try:
        r.connect("sid")
        assert r.subscribe(1, [("si/#", 1)], mp.prop_varint(0x0B, 7)) == [1]
        assert r.subscribe(2, [("si/x", 0)], mp.prop_varint(0x0B, 300)) == [0]
        assert r.subscribe(3, [("si/+", 0)]) == [0]
        a.publish("si/x", "m", qos=1)
        got = []
        for _ in range(3):  # one copy per subscription for a 5.0 client
            p = recv_pub(r)
            got.append((mp.prop(p["props"], 0x0B), p["qos"]))
            if p["qos"]:
                r.send(mp.ack5(4, p["packet_id"]))
        assert sorted(got, key=str) == sorted([(7, 1), (300, 0), (None, 0)], key=str), got
        no_packet(r)
        r.send(mp.disconnect5())
    finally:
        a.close()


def scenario_v5_options(d, port):
    """No Local, Retain As Published, Retain Handling."""
    r = raw(port)
    a = PahoPeer(port, client_id="a")
    try:
        r.connect("opts")
        assert r.subscribe(1, [("nl/1", mp.sub_options(0, no_local=True)), ("nl/2", 0)]) == [0, 0]
        r.send(mp.publish5("nl/1", "own"))
        r.send(mp.publish5("nl/2", "own"))
        p = recv_pub(r)
        assert p["topic"] == "nl/2", p
        no_packet(r)

        a.publish("rap/x", "kept", qos=1, retain=True)  # acknowledged before subscribing
        assert r.subscribe(2, [("rap/x", mp.sub_options(0, rap=True, rh=2))]) == [0]
        no_packet(r)  # retain handling 2: nothing on subscribing
        a.publish("rap/x", "again", qos=1, retain=True)
        p = recv_pub(r)
        assert (p["payload"], p["retain"]) == (b"again", True), "retain as published"
        assert r.subscribe(3, [("rap/x", mp.sub_options(0, rh=1))]) == [0]
        no_packet(r)  # retain handling 1: an existing subscription gets nothing
        assert r.subscribe(4, [("rap/+", mp.sub_options(0, rh=1))]) == [0]
        p = recv_pub(r)
        assert (p["payload"], p["retain"]) == (b"again", True)  # a new one does
        a.publish("rap/x", "plain", qos=1, retain=True)
        got = sorted((recv_pub(r)["retain"]) for _ in range(2))
        assert got == [False, False], "without RAP the flag is cleared"
        assert r.subscribe(5, [("rap/x", mp.sub_options(0, rh=0))]) == [0]
        assert recv_pub(r)["retain"]
        a.publish("rap/x", "", qos=1, retain=True)
        recv_pub(r)
        recv_pub(r)
        r.send(mp.disconnect5())
    finally:
        a.publish("rap/x", "", qos=1, retain=True)  # cleared even when a check failed
        a.close()


def scenario_v5_expiry(d, port):
    a = PahoPeer(port, client_id="a")
    try:
        r = raw(port)
        r.connect("exp", clean=True, props=mp.prop_u32(0x11, 60))
        r.subscribe(1, [("ex/#", 1)])
        r.send(mp.disconnect5())
        r.close()
        r = raw(port)
        r.connect("pub-only")
        r.send(mp.publish5("ex/short", "gone", qos=1, packet_id=1, props=mp.prop_u32(0x02, 1)))
        r.recv_packet()
        r.send(mp.publish5("ex/long", "kept", qos=1, packet_id=2, props=mp.prop_u32(0x02, 60)))
        r.recv_packet()
        r.send(mp.publish5("ex/ret", "short-retained", qos=1, packet_id=3, retain=True, props=mp.prop_u32(0x02, 1)))
        r.recv_packet()
        r.send(mp.disconnect5())
        time.sleep(2.2)
        r = raw(port)
        present, _ = r.connect("exp", clean=False, props=mp.prop_u32(0x11, 60))
        assert present
        p = recv_pub(r)
        left = mp.prop(p["props"], 0x02)
        assert (p["topic"], p["payload"]) == ("ex/long", b"kept") and left is not None and 55 <= left <= 60, p
        r.send(mp.ack5(4, p["packet_id"]))
        no_packet(r)  # ex/short expired while queued, the retained one too
        assert r.subscribe(2, [("ex/ret", 0)]) == [0]
        no_packet(r)
        r.send(mp.disconnect5(props=mp.prop_u32(0x11, 0)))
    finally:
        a.close()


def scenario_v5_session_expiry(d, port):
    r = raw(port)
    r.connect("sx", props=mp.prop_u32(0x11, 1))
    r.subscribe(1, [("sx/#", 1)])
    r.send(mp.disconnect5())
    r = raw(port)
    present, _ = r.connect("sx", clean=False, props=mp.prop_u32(0x11, 1))
    assert present, "kept within the expiry"
    r.send(mp.disconnect5())
    time.sleep(2.2)
    d.stats()  # let the broker run its timers
    r = raw(port)
    present, _ = r.connect("sx", clean=False)
    assert not present, "gone after the expiry"
    # session expiry 0 at CONNECT cannot be raised at DISCONNECT
    r.send(mp.disconnect5(props=mp.prop_u32(0x11, 30)))
    r.expect_disconnect(0x82)


def scenario_v5_will_delay(d, port):
    b = PahoPeer(port, client_id="b")
    try:
        b.subscribe("wd/#", 1)
        r = raw(port)
        r.connect("wd", props=mp.prop_u32(0x11, 30), will=("wd/1", "late", 1, False, mp.prop_u32(0x18, 2)))
        r.close()
        b.expect_none(1.0)
        m = b.expect(timeout=4)
        assert m.payload == b"late"
        # back in time: no will
        r = raw(port)
        r.connect("wd", props=mp.prop_u32(0x11, 30), will=("wd/1", "cancelled", 1, False, mp.prop_u32(0x18, 2)))
        r.close()
        time.sleep(0.5)
        r = raw(port)
        r.connect("wd", clean=False, props=mp.prop_u32(0x11, 30))
        b.expect_none(2.5)
        # DISCONNECT with reason 0x04 publishes the will
        r.send(mp.disconnect5(0x04, mp.prop_u32(0x11, 0)))
        r.close()
        r = raw(port)
        r.connect("wd2", will=("wd/2", "asked-for", 0, False, b""))
        r.send(mp.disconnect5(0x04))
        assert b.expect().payload == b"asked-for"
        # the session ending before the delay publishes the will then
        r = raw(port)
        r.connect("wd3", props=mp.prop_u32(0x11, 1), will=("wd/3", "session-end", 0, False, mp.prop_u32(0x18, 60)))
        r.close()
        m = b.expect(timeout=4)
        assert m.payload == b"session-end"
    finally:
        b.close()


def scenario_v5_topic_alias(d, port):
    b = PahoPeer(port, client_id="b")
    try:
        b.subscribe("ta/#", 0)
        r = raw(port)
        r.connect("alias")
        r.send(mp.publish5("ta/long/topic", "one", props=mp.prop_u16(0x23, 1)))
        r.send(mp.publish5("", "two", props=mp.prop_u16(0x23, 1)))
        assert [(m.topic, m.payload) for m in (b.expect(), b.expect())] == [("ta/long/topic", b"one"), ("ta/long/topic", b"two")]
        r.send(mp.publish5("", "unknown", props=mp.prop_u16(0x23, 2)))
        r.expect_disconnect(0x82)
        r = raw(port)
        r.connect("alias")
        r.send(mp.publish5("ta/x", "too-high", props=mp.prop_u16(0x23, 9)))
        r.expect_disconnect(0x94)
        r = raw(port)
        r.connect("alias")
        r.send(mp.publish5("ta/x", "no", props=mp.prop_varint(0x0B, 1)))  # subscription id from a client
        r.expect_disconnect(0x82)
    finally:
        b.close()


def scenario_v5_reason_codes(d, port):
    r = raw(port)
    r.connect("rc")
    r.send(mp.publish5("rc/nobody", "x", qos=1, packet_id=1))
    assert r.recv_packet() == (0x40, b"\x00\x01\x10"), "no matching subscribers"
    r.send(mp.publish5("rc/bad", b"\xC0\x80", qos=1, packet_id=2, props=mp.prop_u8(0x01, 1)))
    assert r.recv_packet() == (0x40, b"\x00\x02\x99"), "payload not UTF-8"
    d.setting("ACL 1")
    try:
        r.send(mp.publish5("secret/x", "x", qos=1, packet_id=3))
        assert r.recv_packet() == (0x40, b"\x00\x03\x87")
        assert r.subscribe(4, [("secret/#", 0), ("a/#/b", 0), ("ok/#", 2)]) == [0x87, 0x8F, 2]
    finally:
        d.setting("ACL 0")
    r.send(mp.unsubscribe5(5, ["ok/#", "never"]))
    first, body = r.recv_packet()
    assert first == 0xB0 and body == b"\x00\x05\x00\x00\x11", body
    # QoS 2 PUBREL for an unknown id
    r.send(mp.ack5(6, 77))
    assert r.recv_packet() == (0x70, b"\x00\x4d\x92")
    r.send(mp.disconnect5())


def scenario_v5_shared(d, port):
    members = [v5(port, f"sh{i}") for i in range(3)]
    plain = PahoPeer(port, client_id="plain")
    a = PahoPeer(port, client_id="a")
    try:
        a.publish("sh/r", "retained", qos=1, retain=True)  # acknowledged before anyone subscribes
        for m in members:
            m.subscribe("$share/g/sh/#", 1)
        plain.subscribe("sh/#", 0)
        assert plain.expect().payload == b"retained"
        for m in members:
            m.expect_none(0.3)  # no retained messages for shared subscriptions
        for i in range(6):
            a.publish("sh/x", f"m{i}", qos=1)
        got = {}
        for idx, m in enumerate(members):
            got[idx] = []
            while True:
                try:
                    got[idx].append(m.expect(timeout=1).payload)
                except TimeoutError:
                    break
        all_msgs = sorted(p for v in got.values() for p in v)
        assert all_msgs == sorted(f"m{i}".encode() for i in range(6)), got
        assert all(len(v) == 2 for v in got.values()), got  # taken in turn
        assert sorted(plain.expect().payload for _ in range(6)) == sorted(f"m{i}".encode() for i in range(6))
        a.publish("sh/r", "", qos=1, retain=True)
        plain.expect()
        # No Local on a shared subscription is a protocol error
        r = raw(port)
        r.connect("shnl")
        r.send(mp.subscribe5(1, [("$share/g/x", mp.sub_options(0, no_local=True))]))
        r.expect_disconnect(0x82)
    finally:
        a.publish("sh/r", "", qos=1, retain=True)  # cleared even when a check failed
        for m in members:
            m.close()
        plain.close()
        a.close()


def scenario_v5_flow_control(d, port):
    """Receive Maximum and Maximum Packet Size of the client."""
    a = PahoPeer(port, client_id="a")
    try:
        r = raw(port)
        r.connect("flow", props=mp.prop_u16(0x21, 2) + mp.prop_u32(0x27, 100))
        r.subscribe(1, [("fc/#", 1)])
        a.publish("fc/big", "x" * 200, qos=1)  # larger than the client takes: dropped
        for i in range(4):
            a.publish("fc/x", f"m{i}", qos=1)
        p1, p2 = recv_pub(r), recv_pub(r)
        assert [p1["payload"], p2["payload"]] == [b"m0", b"m1"]
        no_packet(r)  # two unacknowledged at most
        r.send(mp.ack5(4, p1["packet_id"]))
        assert recv_pub(r)["payload"] == b"m2"
        no_packet(r)
        r.send(mp.ack5(4, p2["packet_id"]))
        assert recv_pub(r)["payload"] == b"m3"
        r.send(mp.disconnect5())
        # more QoS 2 publishes than the broker's Receive Maximum
        r = raw(port)
        r.connect("flood")
        for i in range(1, d.qos2_in + 2):
            r.send(mp.publish5("fc/q2", "x", qos=2, packet_id=i))
        for _ in range(d.qos2_in):
            first, _ = r.recv_packet()
            assert first == 0x50
        r.expect_disconnect(0x93)
    finally:
        a.close()


def scenario_v5_server_disconnects(d, port):
    r1 = raw(port)
    r1.connect("dup5")
    r2 = raw(port)
    r2.connect("dup5")
    r1.expect_disconnect(0x8E)  # session taken over
    r2.send(mp.disconnect5())
    r = raw(port)
    r.connect("ka5", keepalive=1)
    r.expect_disconnect(0x8D, timeout=4)  # keep alive timeout
    r = raw(port)
    r.connect("big5")
    r.send(mp.publish5("x", "x" * 2000))
    r.expect_disconnect(0x95)


def scenario_v5_enhanced_auth(d, port):
    def method(m, data=None):
        p = mp.prop_str(0x15, m)
        return p + (mp.prop_str(0x16, data) if data is not None else b"")

    d.setting("XAUTH 1")
    try:
        # one step: CONNACK carries the method and the broker's data
        r = raw(port)
        _, props = r.connect("xa1", props=method("TEST-TOKEN", "token"))
        assert mp.prop(props, 0x15) == b"TEST-TOKEN" and mp.prop(props, 0x16) == b"ok", props
        r.send(mp.disconnect5())
        r = raw(port)
        r.connect("xa2", expect_reason=0x87, props=method("TEST-TOKEN", "wrong"))
        assert r.closed_by_peer()
        r = raw(port)
        r.connect("xa3", expect_reason=0x8C, props=method("OTHER"))
        assert r.closed_by_peer()
        # a challenge: CONNECT -> AUTH 0x18 -> AUTH 0x18 -> CONNACK
        r = raw(port)
        r.send(mp.connect5("xa4", props=method("TEST-CHALLENGE", "hello")))
        assert mp.prop(r.expect_auth(0x18), 0x16) == b"nonce"
        r.send(mp.auth5(0x18, method("TEST-CHALLENGE", "nonce-ok")))
        first, body = r.recv_packet()
        assert first == 0x20 and body[1] == 0, (first, body)
        props, _ = mp.parse_props(body, 2)
        assert mp.prop(props, 0x16) == b"welcome", props
        # the connection works, and re-authenticates (0x19) without a break
        assert r.subscribe(1, [("xa/t", 0)]) == [0]
        r.send(mp.auth5(0x19, method("TEST-CHALLENGE", "hello")))
        assert mp.prop(r.expect_auth(0x18), 0x16) == b"nonce"
        r.send(mp.auth5(0x18, method("TEST-CHALLENGE", "nonce-ok")))
        assert mp.prop(r.expect_auth(0x00), 0x16) == b"welcome"
        r.send(mp.publish5("xa/t", "after"))
        assert recv_pub(r)["payload"] == b"after"
        # re-authentication that fails ends the connection
        r.send(mp.auth5(0x19, method("TEST-CHALLENGE", "bye")))
        r.expect_disconnect(0x87)
        # a wrong answer, another method in AUTH, anything but AUTH meanwhile
        r = raw(port)
        r.send(mp.connect5("xa5", props=method("TEST-CHALLENGE", "hello")))
        r.expect_auth(0x18)
        r.send(mp.auth5(0x18, method("TEST-CHALLENGE", "nope")))
        first, body = r.recv_packet()
        assert first == 0x20 and body[1] == 0x87, (first, body)
        assert r.closed_by_peer()
        r = raw(port)
        r.send(mp.connect5("xa6", props=method("TEST-CHALLENGE", "hello")))
        r.expect_auth(0x18)
        r.send(mp.auth5(0x18, method("TEST-TOKEN", "token")))
        first, body = r.recv_packet()
        assert first == 0x20 and body[1] == 0x82, (first, body)
        assert r.closed_by_peer()
        r = raw(port)
        r.send(mp.connect5("xa7", props=method("TEST-CHALLENGE", "hello")))
        r.expect_auth(0x18)
        r.send(mp.publish5("xa/t", "early"))
        first, body = r.recv_packet()
        assert first == 0x20 and body[1] == 0x82, (first, body)
        assert r.closed_by_peer()
        # AUTH from a client that connected without a method
        r = raw(port)
        r.connect("xa8")
        r.send(mp.auth5(0x19, method("TEST-TOKEN", "token")))
        r.expect_disconnect(0x82)
    finally:
        d.setting("XAUTH 0")


SCENARIOS = (
    ("v5_connack", scenario_v5_connack),
    ("v5_properties", scenario_v5_properties),
    ("v5_subscription_ids", scenario_v5_subscription_ids),
    ("v5_options", scenario_v5_options),
    ("v5_expiry", scenario_v5_expiry),
    ("v5_session_expiry", scenario_v5_session_expiry),
    ("v5_will_delay", scenario_v5_will_delay),
    ("v5_topic_alias", scenario_v5_topic_alias),
    ("v5_reason_codes", scenario_v5_reason_codes),
    ("v5_shared", scenario_v5_shared),
    ("v5_flow_control", scenario_v5_flow_control),
    ("v5_server_disconnects", scenario_v5_server_disconnects),
    ("v5_enhanced_auth", scenario_v5_enhanced_auth),
)
