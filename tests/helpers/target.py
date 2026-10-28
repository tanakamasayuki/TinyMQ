"""Where the PC-side peers find the DUT, and where the DUT finds the PC.

The host core shares the PC's network, so both are 127.0.0.1. A board on
Wi-Fi prints its address (READY <ip>); the test sets HOST to it, and the
services the board connects to listen on all addresses.
"""

import socket

HOST = "127.0.0.1"


def is_local(ip):
    return ip.startswith("127.")


def pc_address_for(ip):
    """The PC's address on the route to `ip` (what a board uses to reach the PC)."""
    if is_local(ip):
        return "127.0.0.1"
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect((ip, 9))  # no packet is sent
        return s.getsockname()[0]


def bind_address_for(ip):
    """Address a PC-side service should listen on to be reached from `ip`."""
    return "127.0.0.1" if is_local(ip) else "0.0.0.0"
