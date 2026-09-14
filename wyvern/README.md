# aalink on the EMAX Wyvern Link (camera-side, patched)

`aalink` is the adaptive-link daemon that runs **on the EMAX Wyvern camera**
(OpenIPC / SigmaStar star6e), not on the VRX. It reads the camera's local wlan0
RSSI plus a UDP RSSI feedback from the ground (:12345), maps RSSI → 802.11 MCS
via thresholds, and sets the MCS + the video bitrate on venc.

It is a **manual install** on the camera's persistent overlay (`/usr/bin/aalink`
+ `/etc/aalink.conf`). That survives reboots but **not a full firmware reflash**.
After a reflash, run `install.sh` to put this binary back. (A proper OpenIPC
buildroot package would auto-bake it — not done yet; see "Firmware package" below.)

## Files
- `../aalink.c` — full source with our fixes.
- `aalink.star6e` — cross-compiled ARM hard-float binary (md5 `45ff923b6425`).
  Build output, gitignored; an archived copy of the deployed binary is kept on
  Drive under `Archive/Tech/Drone/OpenIPC/Sigmastar/deployed/`.
- `aalink.conf` — the last `/etc/aalink.conf` kept on the PC (2026-08-21). The
  camera was live-tuned during field tests, so compare it with the file on the
  camera before overwriting either one.
- `install.sh` — md5-verified re-deploy to the camera via the VRX socat proxy.

Build, from the repository root:
`arm-buildroot-linux-gnueabihf-gcc -O2 -march=armv7-a -mfpu=neon -mfloat-abi=hard -o wyvern/aalink.star6e aalink.c -lpthread -lm`
(toolchain: `~/tc/armv7-eabihf--glibc--stable-2023.11-1` in WSL).

## What this binary changes vs upstream sickgreg/aalink
1. **Recovery fix** — on a rising signal, `rssi_local` tracks the FAST EMA
   instead of the lagging slow EMA, so the link/bitrate climbs back after a dip
   (still cooldown/confidence/hysteresis-gated on the way up — convergent, not
   instant). Upstream fell back to the slow EMA and effectively never recovered.
2. **ROI degrade step** — as the commanded bitrate falls, shrink `fpv.roiCenter`
   (0.90 → 0.20) alongside the existing roiQp ramp, concentrating quality in the
   center. Gated: only when ROI is in use (`SIMPLE_ROIQP != 0`) and NOT during
   thermal throttle. Shares the degrade band with the roiQp ramp.
3. **Idempotent resync (P4)** — a periodic resync in the main loop re-affirms the
   bitrate for the current (mcs,width) even on a stable link, so config reloads /
   a stuck value / a failed command self-correct without waiting for an MCS
   change. Dispatched to the single `mcs_worker` writer via the existing
   `mcs_lock` (NO new lock). `update_bitrate` self-guards on the last
   successfully-commanded kbps, so it never spams venc and retries a failure.

## Key /etc/aalink.conf settings we run (verify/set after a reflash)
```
PR=100,600,1200,3250,6500,11000,11000,11000,11000,11000,11000,11000   # ~20 Mbps cap @40MHz
THROUGHPUT_PCT=90
MCS_SOURCE=lowest
MAX_MCS_40_EU=4            # (and _AU/_BU/_CU = 4)
SIMPLE_ROIQP=-17          # ROI center-priority enabled
UP_COOLDOWN_MS=300
UPWARD_CONFIDENCE_LOOPS=2
H=10
THRESH_SHIFT=8
PREDICT_MULTI=3.0         # aggressive predictive drop (fall-fast)
EXTERNAL_OSD=0
API_CMD=/usr/bin/gslink %d   # gslink lives in wyvern-ai-osd, link/gslink/
```

## Firmware package (not done — future)
To survive a reflash automatically, add an OpenIPC buildroot package under
`~/openipc-dev/firmware/general/package/` that installs `aalink` + a default
`aalink.conf`, then rebuild the camera firmware. Bigger job; the manual overlay
install + `install.sh` is the current, working persistence.
