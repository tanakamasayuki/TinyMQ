"""MQTT 3.1.1 packets built and parsed by hand, for raw-socket peers."""

import socket
import struct

from helpers import target


def encode_length(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def string(s):
    if isinstance(s, str):
        s = s.encode()
    return struct.pack(">H", len(s)) + s


def packet(first, body):
    return bytes([first]) + encode_length(len(body)) + body


def connect(client_id, clean=True, keepalive=60, will=None, username=None, password=None, level=4, name="MQTT"):
    """`will` is (topic, payload, qos, retain)."""
    flags = 0x02 if clean else 0
    body = string(name) + bytes([level])
    tail = string(client_id)
    if will:
        topic, payload, qos, retain = will
        flags |= 0x04 | (qos << 3) | (0x20 if retain else 0)
        tail += string(topic) + string(payload)
    if username is not None:
        flags |= 0x80
        tail += string(username)
    if password is not None:
        flags |= 0x40
        tail += string(password)
    body += bytes([flags]) + struct.pack(">H", keepalive) + tail
    return packet(0x10, body)


def connack(session_present=False, code=0):
    return bytes([0x20, 0x02, 1 if session_present else 0, code])


def ack(packet_type, packet_id):
    """PUBACK 4, PUBREC 5, PUBREL 6, PUBCOMP 7."""
    first = (packet_type << 4) | (2 if packet_type == 6 else 0)
    return bytes([first, 0x02]) + struct.pack(">H", packet_id)


def publish(topic, payload, qos=0, packet_id=0, dup=False, retain=False):
    if isinstance(payload, str):
        payload = payload.encode()
    body = string(topic)
    if qos:
        body += struct.pack(">H", packet_id)
    body += payload
    return packet(0x30 | (0x08 if dup else 0) | (qos << 1) | (1 if retain else 0), body)


def subscribe(packet_id, filters):
    """`filters` is [(filter, qos), ...]."""
    body = struct.pack(">H", packet_id)
    for f, q in filters:
        body += string(f) + bytes([q])
    return packet(0x82, body)


def unsubscribe(packet_id, filters):
    body = struct.pack(">H", packet_id)
    for f in filters:
        body += string(f)
    return packet(0xA2, body)


PINGREQ = bytes([0xC0, 0x00])
DISCONNECT = bytes([0xE0, 0x00])


def parse_publish(first, body):
    """Returns (topic, packet id, payload, qos, dup, retain)."""
    qos = (first >> 1) & 3
    tlen = struct.unpack(">H", body[:2])[0]
    topic = body[2 : 2 + tlen].decode()
    pos = 2 + tlen
    pid = 0
    if qos:
        pid = struct.unpack(">H", body[pos : pos + 2])[0]
        pos += 2
    return topic, pid, body[pos:], qos, bool(first & 0x08), bool(first & 0x01)


class PacketSocket:
    """Reads whole packets from a connected socket."""

    def __init__(self, sock, timeout=5):
        self.sock = sock
        self.sock.settimeout(timeout)

    def _read_exact(self, n):
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("closed by the peer")
            data += chunk
        return data

    def recv_packet(self, timeout=None):
        """Returns (first byte, body)."""
        if timeout is not None:
            self.sock.settimeout(timeout)
        first = self._read_exact(1)[0]
        length, shift = 0, 0
        while True:
            b = self._read_exact(1)[0]
            length |= (b & 0x7F) << shift
            shift += 7
            if not b & 0x80:
                break
        return first, self._read_exact(length)

    def send(self, data):
        self.sock.sendall(data)

    def closed_by_peer(self, timeout=5):
        """Waits until the peer closes; True if it did, False on timeout."""
        self.sock.settimeout(timeout)
        try:
            while True:
                data = self.sock.recv(4096)
                if not data:
                    return True
        except (ConnectionResetError, BrokenPipeError):
            return True
        except socket.timeout:
            return False

    def close(self):
        self.sock.close()


class RawClient(PacketSocket):
    """A hand-driven MQTT client."""

    def __init__(self, port, host=None, timeout=5):
        super().__init__(socket.create_connection((host or target.HOST, port), timeout=timeout), timeout)

    def connect(self, client_id, expect_code=0, **kwargs):
        """Sends CONNECT and returns session present."""
        self.send(connect(client_id, **kwargs))
        first, body = self.recv_packet()
        assert first == 0x20 and len(body) == 2, (first, body)
        assert body[1] == expect_code, f"CONNACK code {body[1]}"
        return bool(body[0] & 1)

    def subscribe(self, packet_id, filters):
        self.send(subscribe(packet_id, filters))
        first, body = self.recv_packet()
        assert first == 0x90 and body[:2] == struct.pack(">H", packet_id), (first, body)
        return list(body[2:])


# ---- MQTT 5.0


def varint(n):
    return encode_length(n)


def prop_u8(pid, v):
    return bytes([pid, v])


def prop_u16(pid, v):
    return bytes([pid]) + struct.pack(">H", v)


def prop_u32(pid, v):
    return bytes([pid]) + struct.pack(">I", v)


def prop_varint(pid, v):
    return bytes([pid]) + varint(v)


def prop_str(pid, s):
    return bytes([pid]) + string(s)


def prop_pair(pid, k, v):
    return bytes([pid]) + string(k) + string(v)


def props_block(props=b""):
    return varint(len(props)) + props


_TYPES = {
    0x01: "u8", 0x02: "u32", 0x03: "str", 0x08: "str", 0x09: "bin", 0x0B: "varint", 0x11: "u32", 0x12: "str",
    0x13: "u16", 0x15: "str", 0x16: "bin", 0x17: "u8", 0x18: "u32", 0x19: "u8", 0x1A: "str", 0x1C: "str",
    0x1F: "str", 0x21: "u16", 0x22: "u16", 0x23: "u16", 0x24: "u8", 0x25: "u8", 0x26: "pair", 0x27: "u32",
    0x28: "u8", 0x29: "u8", 0x2A: "u8",
}


def read_varint(data, pos):
    value, shift = 0, 0
    while True:
        b = data[pos]
        pos += 1
        value |= (b & 0x7F) << shift
        shift += 7
        if not b & 0x80:
            return value, pos


def parse_props(data, pos=0):
    """Reads a property length and block at `pos`. Returns ([(id, value)], next pos)."""
    length, pos = read_varint(data, pos)
    end = pos + length
    out = []
    while pos < end:
        pid = data[pos]
        pos += 1
        t = _TYPES[pid]
        if t == "u8":
            v = data[pos]
            pos += 1
        elif t == "u16":
            v = struct.unpack(">H", data[pos : pos + 2])[0]
            pos += 2
        elif t == "u32":
            v = struct.unpack(">I", data[pos : pos + 4])[0]
            pos += 4
        elif t == "varint":
            v, pos = read_varint(data, pos)
        elif t in ("str", "bin"):
            n = struct.unpack(">H", data[pos : pos + 2])[0]
            v = data[pos + 2 : pos + 2 + n]
            pos += 2 + n
        else:
            n = struct.unpack(">H", data[pos : pos + 2])[0]
            k = data[pos + 2 : pos + 2 + n]
            pos += 2 + n
            n = struct.unpack(">H", data[pos : pos + 2])[0]
            v = (k, data[pos + 2 : pos + 2 + n])
            pos += 2 + n
        out.append((pid, v))
    return out, end


def prop(props, pid, default=None):
    for p, v in props:
        if p == pid:
            return v
    return default


def connect5(client_id, clean=True, keepalive=60, props=b"", will=None, username=None, password=None):
    """`will` is (topic, payload, qos, retain, will properties)."""
    flags = 0x02 if clean else 0
    tail = string(client_id)
    if will:
        topic, payload, qos, retain, wprops = will
        flags |= 0x04 | (qos << 3) | (0x20 if retain else 0)
        tail += props_block(wprops) + string(topic) + string(payload)
    if username is not None:
        flags |= 0x80
        tail += string(username)
    if password is not None:
        flags |= 0x40
        tail += string(password)
    body = string("MQTT") + bytes([5, flags]) + struct.pack(">H", keepalive) + props_block(props) + tail
    return packet(0x10, body)


def publish5(topic, payload, qos=0, packet_id=0, props=b"", retain=False, dup=False):
    if isinstance(payload, str):
        payload = payload.encode()
    body = string(topic)
    if qos:
        body += struct.pack(">H", packet_id)
    body += props_block(props) + payload
    return packet(0x30 | (0x08 if dup else 0) | (qos << 1) | (1 if retain else 0), body)


def subscribe5(packet_id, filters, props=b""):
    """`filters` is [(filter, options byte), ...]."""
    body = struct.pack(">H", packet_id) + props_block(props)
    for f, o in filters:
        body += string(f) + bytes([o])
    return packet(0x82, body)


def unsubscribe5(packet_id, filters, props=b""):
    body = struct.pack(">H", packet_id) + props_block(props)
    for f in filters:
        body += string(f)
    return packet(0xA2, body)


def ack5(packet_type, packet_id, reason=0):
    first = (packet_type << 4) | (2 if packet_type == 6 else 0)
    return bytes([first, 0x03]) + struct.pack(">HB", packet_id, reason)


def disconnect5(reason=0, props=b""):
    return packet(0xE0, bytes([reason]) + props_block(props))


def sub_options(qos=0, no_local=False, rap=False, rh=0):
    return qos | (4 if no_local else 0) | (8 if rap else 0) | (rh << 4)


def parse_publish5(first, body):
    """Returns a dict: topic, packet_id, props, payload, qos, dup, retain."""
    qos = (first >> 1) & 3
    tlen = struct.unpack(">H", body[:2])[0]
    topic = body[2 : 2 + tlen].decode()
    pos = 2 + tlen
    pid = 0
    if qos:
        pid = struct.unpack(">H", body[pos : pos + 2])[0]
        pos += 2
    props, pos = parse_props(body, pos)
    return {"topic": topic, "packet_id": pid, "props": props, "payload": body[pos:], "qos": qos, "dup": bool(first & 8), "retain": bool(first & 1)}


class RawClient5(RawClient):
    """A hand-driven MQTT 5.0 client."""

    def connect(self, client_id, expect_reason=0, **kwargs):
        """Sends CONNECT; returns (session present, CONNACK properties)."""
        self.send(connect5(client_id, **kwargs))
        first, body = self.recv_packet()
        assert first == 0x20, (first, body)
        assert body[1] == expect_reason, f"CONNACK reason 0x{body[1]:02x}"
        props, _ = parse_props(body, 2)
        return bool(body[0] & 1), props

    def subscribe(self, packet_id, filters, props=b""):
        """Returns the SUBACK reason codes."""
        self.send(subscribe5(packet_id, filters, props))
        first, body = self.recv_packet()
        assert first == 0x90 and body[:2] == struct.pack(">H", packet_id), (first, body)
        _, pos = parse_props(body, 2)
        return list(body[pos:])

    def expect_disconnect(self, reason, timeout=5):
        """Waits for a DISCONNECT with `reason`, then for the close."""
        first, body = self.recv_packet(timeout)
        assert first == 0xE0 and body[:1] == bytes([reason]), (first, body)
        assert self.closed_by_peer(timeout)
