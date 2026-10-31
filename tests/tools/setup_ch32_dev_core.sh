#!/bin/sh
# Links the unreleased ArduinoCore-CH32RV work tree into a sketchbook of its
# own (tests/.tools/ch32-dev), for the ch32v307big profile: CH32V307 with
# Flash / RAM split 192K / 128K, which 0.4.0 does not have yet. Run the tests
# with that sketchbook:
#
#   tests/tools/setup_ch32_dev_core.sh
#   ARDUINO_DIRECTORIES_USER=$PWD/.tools/ch32-dev uv run --env-file .env pytest ... --profile ch32v307big
#
# The board has little RAM left beside lwIP, so everything built from this
# sketchbook gets -DTINYMQ_PRESET_SMALL (the plugin has no per-profile
# defines; the core leaves compiler.<lang>.extra_flags to the user).
#
# The core sets the option bytes to the image's split at boot (one extra
# reset) and back to 256K / 64K for an image built with the default split.
set -eu

core=${CH32RV_CORE:-/home/mt/dev_wch/ArduinoCore-CH32RV}
tool=${CH32RV_TOOL_VERSION:-0.24.0}
gcc=${CH32RV_GCC_VERSION:-14.3.0-1}
dest=$(cd "$(dirname "$0")/.." && pwd)/.tools/ch32-dev
platform=$dest/hardware/ch32-riscv-ug/ch32rv

[ -d "$core/cores" ] || { echo "no core at $core (set CH32RV_CORE)" >&2; exit 1; }
rm -rf "$platform"
mkdir -p "$platform"
for name in cores variants libraries platform.txt boards.txt programmers.txt; do
    ln -s "$core/$name" "$platform/$name"
done
cat > "$platform/platform.local.txt" <<EOF
compiler.path=$core/.tools/xpack-riscv-none-elf-gcc/$gcc/bin/
runtime.tools.ch32rv.path=$core/.tools/ch32rv/$tool
tools.ch32rv.path=$core/.tools/ch32rv/$tool
tools.ch32rv.cmd=ch32rv
compiler.c.extra_flags=-DTINYMQ_PRESET_SMALL
compiler.cpp.extra_flags=-DTINYMQ_PRESET_SMALL
EOF
echo "core $(git -C "$core" rev-parse --short HEAD) linked into $dest"
