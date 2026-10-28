"""A hand-driven MQTT-SN 1.2 client over UDP, for testing the gateway."""

import socket
import struct

from helpers import target

ADVERTISE, SEARCHGW, GWINFO = 0x00, 0x01, 0x02
CONNECT, CONNACK = 0x04, 0x05
WILLTOPICREQ, WILLTOPIC, WILLMSGREQ, WILLMSG = 0x06, 0x07, 0x08, 0x09
REGISTER, REGACK, PUBLISH, PUBACK = 0x0A, 0x0B, 0x0C, 0x0D
PUBCOMP, PUBREC, PUBREL = 0x0E, 0x0F, 0x10
SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK = 0x12, 0x13, 0x14, 0x15
PINGREQ, PINGRESP, DISCONNECT = 0x16, 0x17, 0x18
WILLTOPICUPD, WILLTOPICRESP, WILLMSGUPD, WILLMSGRESP = 0x1A, 0x1B, 0x1C, 0x1D

NORMAL, PREDEFINED, SHORT = 0, 1, 2


def flags(qos=0, retain=False, will=False, clean=False, topic_type=NORMAL, dup=False):
    q = 3 if qos == -1 else qos
    return (0x80 if dup else 0) | (q << 5) | (0x10 if retain else 0) | (0x08 if will else 0) | (0x04 if clean else 0) | topic_type


def parse_flags(b):
    q = (b >> 5) & 3
    return {"dup": bool(b & 0x80), "qos": -1 if q == 3 else q, "retain": bool(b & 0x10), "topic_type": b & 3}


def frame(msg_type, body=b""):
    total = len(body) + 2
    if total < 256:
        return bytes([total, msg_type]) + body
    return bytes([0x01]) + struct.pack(">H", total + 2) + bytes([msg_type]) + body


def short_id(name):
    assert len(name) == 2
    return (ord(name[0]) << 8) | ord(name[1])


class SnPeer:
    def __init__(self, port, host=None, timeout=5):
        self.addr = (host or target.HOST, port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((target.bind_address_for(self.addr[0]), 0))
        self.sock.settimeout(timeout)
        self.msg_id = 0
        self.topics = {}  # topic id -> name, from REGISTER / REGACK / SUBACK
        self.pending = []  # messages received while waiting for another type

    def close(self):
        self.sock.close()

    def next_id(self):
        self.msg_id = self.msg_id % 0xFFFF + 1
        return self.msg_id

    def send(self, msg_type, body=b""):
        self.sock.sendto(frame(msg_type, body), self.addr)

    def recv(self, timeout=5):
        """Returns (type, body) of the next datagram."""
        if self.pending:
            return self.pending.pop(0)
        self.sock.settimeout(timeout)
        data, _ = self.sock.recvfrom(4096)
        if data[0] == 0x01:
            total = struct.unpack(">H", data[1:3])[0]
            assert total == len(data)
            return data[3], data[4:]
        assert data[0] == len(data), data
        return data[1], data[2:]

    def expect(self, msg_type, timeout=5):
        """The next message of `msg_type`; others are kept for later."""
        kept = []
        try:
            while True:
                t, body = self.recv(timeout)
                if t == msg_type:
                    return body
                kept.append((t, body))
        finally:
            self.pending = kept + self.pending

    def expect_nothing(self, wait=0.5):
        try:
            t, body = self.recv(wait)
        except socket.timeout:
            return
        raise AssertionError(f"unexpected message 0x{t:02x} {body!r}")

    # ---- procedures

    def connect(self, client_id, clean=True, duration=60, will=None, protocol=1):
        """`will` is (topic, message, qos, retain). Returns the CONNACK return code."""
        body = bytes([flags(clean=clean, will=bool(will)), protocol]) + struct.pack(">H", duration) + client_id.encode()
        self.send(CONNECT, body)
        if will:
            topic, message, qos, retain = will
            self.expect(WILLTOPICREQ)
            self.send(WILLTOPIC, bytes([flags(qos=qos, retain=retain)]) + topic.encode())
            self.expect(WILLMSGREQ)
            self.send(WILLMSG, message.encode() if isinstance(message, str) else message)
        return self.expect(CONNACK)[0]

    def register(self, name):
        mid = self.next_id()
        self.send(REGISTER, struct.pack(">HH", 0, mid) + name.encode())
        tid, got_mid, rc = struct.unpack(">HHB", self.expect(REGACK))
        assert got_mid == mid and rc == 0, (got_mid, rc)
        self.topics[tid] = name
        return tid

    def publish(self, topic_id, data, qos=0, topic_type=NORMAL, retain=False, msg_id=None, dup=False):
        """Sends PUBLISH and completes the QoS 1 / 2 exchange. Returns the PUBACK return code (0 for QoS 0 / -1 / 2)."""
        if isinstance(data, str):
            data = data.encode()
        mid = 0 if qos <= 0 else (msg_id or self.next_id())
        self.send(PUBLISH, bytes([flags(qos=qos, retain=retain, topic_type=topic_type, dup=dup)]) + struct.pack(">HH", topic_id, mid) + data)
        if qos == 1:
            tid, got_mid, rc = struct.unpack(">HHB", self.expect(PUBACK))
            assert got_mid == mid and tid == topic_id, (tid, got_mid)
            return rc
        if qos == 2:
            assert struct.unpack(">H", self.expect(PUBREC))[0] == mid
            self.send(PUBREL, struct.pack(">H", mid))
            assert struct.unpack(">H", self.expect(PUBCOMP))[0] == mid
        return 0

    def subscribe(self, topic, qos=0, topic_type=NORMAL):
        """`topic` is a name (NORMAL, SHORT) or an id (PREDEFINED). Returns (granted QoS, topic id, rc)."""
        mid = self.next_id()
        if topic_type == NORMAL:
            tail = topic.encode()
        elif topic_type == SHORT:
            tail = struct.pack(">H", short_id(topic))
        else:
            tail = struct.pack(">H", topic)
        self.send(SUBSCRIBE, bytes([flags(qos=qos, topic_type=topic_type)]) + struct.pack(">H", mid) + tail)
        body = self.expect(SUBACK)
        f, tid, got_mid, rc = struct.unpack(">BHHB", body)
        assert got_mid == mid
        if topic_type == NORMAL and tid:
            self.topics[tid] = topic
        return parse_flags(f)["qos"], tid, rc

    def unsubscribe(self, topic, topic_type=NORMAL):
        mid = self.next_id()
        tail = topic.encode() if topic_type == NORMAL else struct.pack(">H", topic if topic_type == PREDEFINED else short_id(topic))
        self.send(UNSUBSCRIBE, bytes([flags(topic_type=topic_type)]) + struct.pack(">H", mid) + tail)
        assert struct.unpack(">H", self.expect(UNSUBACK))[0] == mid

    def receive(self, ack=True, timeout=5):
        """Waits for a PUBLISH, answering REGISTER on the way. Returns a dict with
        topic (a name when known), topic_id, data, qos, retain, dup, msg_id."""
        while True:
            t, body = self.recv(timeout)
            if t == REGISTER:
                tid, mid = struct.unpack(">HH", body[:4])
                self.topics[tid] = body[4:].decode()
                self.send(REGACK, struct.pack(">HHB", tid, mid, 0))
                continue
            if t != PUBLISH:
                self.pending.append((t, body))
                continue
            f = parse_flags(body[0])
            tid, mid = struct.unpack(">HH", body[1:5])
            if f["topic_type"] == SHORT:
                topic = chr(tid >> 8) + chr(tid & 0xFF)
            else:
                topic = self.topics.get(tid) if f["topic_type"] == NORMAL else tid
            msg = {"topic": topic, "topic_id": tid, "data": body[5:], "msg_id": mid, **f}
            if ack:
                self.ack(msg)
            return msg

    def ack(self, msg):
        if msg["qos"] == 1:
            self.send(PUBACK, struct.pack(">HHB", msg["topic_id"], msg["msg_id"], 0))
        elif msg["qos"] == 2:
            self.send(PUBREC, struct.pack(">H", msg["msg_id"]))
            assert struct.unpack(">H", self.expect(PUBREL))[0] == msg["msg_id"]
            self.send(PUBCOMP, struct.pack(">H", msg["msg_id"]))

    def ping(self, client_id=None):
        self.send(PINGREQ, client_id.encode() if client_id else b"")

    def disconnect(self, duration=None):
        self.send(DISCONNECT, struct.pack(">H", duration) if duration is not None else b"")
        self.expect(DISCONNECT)
