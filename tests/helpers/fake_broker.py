"""A scripted MQTT server on a raw socket, for situations a real broker
does not produce on demand (withheld acknowledgements, dropped connections,
duplicate deliveries, silence)."""

import socket

from helpers.mosquitto import free_port
from helpers.mqtt_packets import ack, connack, parse_publish, publish  # noqa: F401  (used through this module)


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

