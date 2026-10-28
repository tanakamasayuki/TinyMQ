"""A scripted MQTT server on a raw socket, for situations a real broker
does not produce on demand (withheld acknowledgements, dropped connections,
duplicate deliveries, silence)."""

import socket
import struct

from helpers.mosquitto import free_port


class FakeBroker:
    def __init__(self):
        self.port = free_port()
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", self.port))
        self.listener.listen(1)
        self.conn = None

    def accept(self, timeout=10):
        self.listener.settimeout(timeout)
        self.conn, _ = self.listener.accept()
        self.conn.settimeout(timeout)

    def _read_exact(self, n):
        data = b""
        while len(data) < n:
            chunk = self.conn.recv(n - len(data))
            if not chunk:
                raise ConnectionError("closed by the client")
            data += chunk
        return data

    def recv_packet(self):
        """Returns (first byte, body)."""
        first = self._read_exact(1)[0]
        length, shift = 0, 0
        while True:
            b = self._read_exact(1)[0]
            length |= (b & 0x7F) << shift
            shift += 7
            if not b & 0x80:
                break
        return first, self._read_exact(length)

    def expect_closed(self, timeout=10):
        """Waits for the client to close the connection."""
        self.conn.settimeout(timeout)
        try:
            data = self.conn.recv(1)
        except ConnectionResetError:
            return True
        return data == b""

    def send(self, data):
        self.conn.sendall(data)

    def drop(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def close(self):
        self.drop()
        self.listener.close()


def connack(session_present=False, code=0):
    return bytes([0x20, 0x02, 1 if session_present else 0, code])


def ack(packet_type, packet_id):
    first = (packet_type << 4) | (2 if packet_type == 6 else 0)
    return bytes([first, 0x02]) + struct.pack(">H", packet_id)


def publish(topic, payload, qos=0, packet_id=0, dup=False, retain=False):
    body = struct.pack(">H", len(topic)) + topic.encode()
    if qos:
        body += struct.pack(">H", packet_id)
    body += payload
    first = 0x30 | (0x08 if dup else 0) | (qos << 1) | (1 if retain else 0)
    return bytes([first]) + encode_length(len(body)) + body


def encode_length(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def parse_publish(first, body):
    """Returns (topic, packet id, payload, qos, dup, retain) of a 3.1.1 PUBLISH."""
    qos = (first >> 1) & 3
    tlen = struct.unpack(">H", body[:2])[0]
    topic = body[2 : 2 + tlen].decode()
    pos = 2 + tlen
    pid = 0
    if qos:
        pid = struct.unpack(">H", body[pos : pos + 2])[0]
        pos += 2
    return topic, pid, body[pos:], qos, bool(first & 0x08), bool(first & 0x01)
