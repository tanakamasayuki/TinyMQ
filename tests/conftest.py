"""Locks for the bench partner of the board under test.

On the shared bench some boards are wired in pairs (IR, GPIO, USB), and the
partner's firmware may drive the shared lines. The bench rule is to hold the
partner's device lock too, even though TinyMQ only uses the network. The
partner is not a peer here (nothing is uploaded to it), so this takes the same
lock file the plugin would take for it, from build to the end of the module.

    TEST_PARTNER_PORT_<PROFILE>=/run/board-identify/by-id/...
"""

from __future__ import annotations

import os

import pytest
from pytest_embedded_arduino_cli.device_lock import DeviceLockError, DeviceLockInfo, DeviceLockSet, default_lock_dir


@pytest.fixture(scope="module", autouse=True)
def arduino_cli_build(request, arduino_cli_build):
    # Wraps the plugin's fixture of the same name, so the lock is held
    # before arduino_cli_upload (which depends on it) writes the board.
    config = request.config
    port = config.option.port
    if not port or "://" in port or config.getoption("device_lock") == "off" or config.getoption("run_mode") == "build":
        yield
        return
    profile = request.getfixturevalue("arduino_cli_app").profile
    partner = os.environ.get(f"TEST_PARTNER_PORT_{profile.upper()}")
    if not partner:
        yield
        return
    lock_dir = config.getoption("device_lock_dir")
    locks = DeviceLockSet(
        [DeviceLockInfo(key=partner, port=partner, profile=profile, sketch_dir=str(request.path.parent), role="partner")],
        lock_dir=lock_dir or default_lock_dir(),
        timeout=config.getoption("device_lock_timeout"),
    )
    try:
        locks.acquire()
    except DeviceLockError as e:
        raise pytest.UsageError(str(e)) from e
    try:
        yield
    finally:
        locks.release()
