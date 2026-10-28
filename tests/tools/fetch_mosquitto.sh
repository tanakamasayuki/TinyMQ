#!/bin/sh
# Unpacks mosquitto from the Debian / Ubuntu archive into tests/.tools/mosquitto
# for the interop tests, without root and without installing it. Libraries
# the system already has are not fetched. helpers/mosquitto.py finds it there
# when no mosquitto is on PATH.
set -e
cd "$(dirname "$0")/.."
dest="$PWD/.tools/mosquitto"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

need=mosquitto
for p in $(apt-cache depends mosquitto | awk '$1 ~ /:$/ && $2 ~ /^lib/ {print $2}'); do
    dpkg -s "$p" >/dev/null 2>&1 || need="$need $p"
done
echo "fetching: $need"
(cd "$tmp" && apt-get download $need)
mkdir -p "$dest"
for f in "$tmp"/*.deb; do
    dpkg -x "$f" "$dest"
done
echo "unpacked into $dest"
