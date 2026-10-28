"""Runs a local mosquitto for the interop tests.

mosquitto is taken from $MOSQUITTO, PATH (including /usr/sbin), or
tests/.tools/mosquitto as unpacked by tools/fetch_mosquitto.sh. The test is
skipped when none is found.
"""

import os
import shutil
import socket
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

TOOLS = Path(__file__).resolve().parent.parent / ".tools" / "mosquitto"


def _find(name, sbin=True):
    """Returns (path, extra environment) for a mosquitto program, or (None, {})."""
    env_path = os.environ.get(name.upper())
    if env_path:
        return Path(env_path), {}
    found = shutil.which(name) or shutil.which(name, path="/usr/sbin:/usr/local/sbin")
    if found:
        return Path(found), {}
    local = TOOLS / ("usr/sbin" if sbin else "usr/bin") / name
    if local.exists():
        libdirs = sorted({str(f.parent) for f in TOOLS.rglob("*.so*")})
        old = os.environ.get("LD_LIBRARY_PATH")
        return local, {"LD_LIBRARY_PATH": ":".join(libdirs + ([old] if old else []))}
    return None, {}


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def make_password_file(path, users):
    """Writes a mosquitto password file for {user: password}."""
    exe, env = _find("mosquitto_passwd", sbin=False)
    if exe is None:
        pytest.skip("mosquitto_passwd not found")
    path.write_text("")
    for user, password in users.items():
        subprocess.run([str(exe), "-b", str(path), user, password], check=True, env={**os.environ, **env})
    path.chmod(0o600)


@contextmanager
def mosquitto(workdir, extra_conf="", allow_anonymous=True):
    """Starts mosquitto on a free port of 127.0.0.1 and yields (port, log path)."""
    exe, env = _find("mosquitto")
    if exe is None:
        pytest.skip("mosquitto not found (run tests/tools/fetch_mosquitto.sh)")
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    port = free_port()
    conf = workdir / f"mosquitto-{port}.conf"
    conf.write_text(
        f"listener {port} 127.0.0.1\n"
        f"allow_anonymous {'true' if allow_anonymous else 'false'}\n"
        "persistence false\n"
        "log_type all\n"
        f"{extra_conf}\n"
    )
    log_path = workdir / f"mosquitto-{port}.log"
    log = open(log_path, "w")
    proc = subprocess.Popen([str(exe), "-c", str(conf), "-v"], stdout=log, stderr=subprocess.STDOUT, env={**os.environ, **env})
    try:
        deadline = time.time() + 5
        while True:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                break
            except OSError:
                if proc.poll() is not None or time.time() > deadline:
                    raise RuntimeError(f"mosquitto did not start, see {log_path}")
                time.sleep(0.05)
        yield port, log_path
    finally:
        proc.terminate()
        try:
            proc.wait(5)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()
