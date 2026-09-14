#!/bin/bash
# Re-deploy our patched aalink onto the EMAX Wyvern camera (OpenIPC/star6e).
# aalink is a MANUAL install on the camera's persistent overlay - it survives
# reboots, but a full firmware reflash reverts it. Run this after a reflash.
#
# Path to the camera: the VRX (192.168.5.1) proxies to the camera (192.168.0.1)
# via socat on :2201 (the camera has no direct route from the PC in gadget mode).
# Camera SSH password = 12345.
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="$HERE/aalink.star6e"        # md5 45ff923b6425 = recovery + ROI(gated) + P4 resync
VRX=192.168.5.1

echo "== ensuring socat proxy VRX:2201 -> camera:22 =="
sshpass -p 12345 ssh -p 22 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "root@$VRX" \
  "pgrep -f TCP-LISTEN:2201 >/dev/null 2>&1 || (socat TCP-LISTEN:2201,fork,reuseaddr TCP:192.168.0.1:22 & ); \
   ping -c1 -W2 192.168.0.1 >/dev/null 2>&1 && echo CAM_UP || { echo CAM_DOWN; exit 1; }"

echo "== copying binary (md5-verified) =="
SRC_MD5=$(md5sum "$BIN" | cut -d' ' -f1)
cat "$BIN" | sshpass -p 12345 ssh -p 2201 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "root@$VRX" "cat > /tmp/aalink.new"
DST_MD5=$(sshpass -p 12345 ssh -p 2201 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "root@$VRX" "md5sum /tmp/aalink.new | cut -d' ' -f1")
[ "$SRC_MD5" = "$DST_MD5" ] || { echo "MD5 MISMATCH ($SRC_MD5 != $DST_MD5)"; exit 1; }
echo "md5 ok: $DST_MD5"

echo "== backup original + install + restart =="
sshpass -p 12345 ssh -p 2201 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "root@$VRX" \
  "[ -f /usr/bin/aalink.orig ] || cp -a /usr/bin/aalink /usr/bin/aalink.orig; \
   chmod 755 /tmp/aalink.new; cp /tmp/aalink.new /usr/bin/aalink; \
   /etc/init.d/S991aalink restart >/dev/null 2>&1; echo installed+restarted"

echo "== config: verify these keys in /etc/aalink.conf (see README) =="
sshpass -p 12345 ssh -p 2201 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "root@$VRX" \
  "grep -E '^PR=|^THROUGHPUT_PCT|^MCS_SOURCE|^MAX_MCS_40_EU|^SIMPLE_ROIQP|^UP_COOLDOWN_MS|^UPWARD_CONFIDENCE_LOOPS' /etc/aalink.conf"

echo "done. Rollback: cp /usr/bin/aalink.orig /usr/bin/aalink && restart"
