# aalink on the EMAX Wyvern Link (camera-side, patched)

`aalink` is the adaptive-link daemon that runs **on the EMAX Wyvern camera**
(OpenIPC / SigmaStar star6e, waybeam 0.84), not on the VRX. It reads the
camera's local wlan0 RSSI plus the ground's feedback on UDP :12345 (RSSI, and
the video loss pcpilot measures), maps RSSI to an 802.11 MCS through
thresholds, sets MCS and TX power, and hands the bitrate it derives to
`gslink` (wyvern-ai-osd, `link/gslink/`), which applies it to waybeam.

It is a **manual install** on the camera's persistent overlay (`/usr/bin/aalink`
+ `/etc/aalink.conf`): it survives reboots but not a full firmware reflash.

## Files
- `../aalink.c` - the source with our changes.
- `aalink.conf` - the Wyvern kit: what the camera flies with, apart from
  values tuned live on it. The camera's own copy is kept, as flown, in
  wyvern-ai-osd `camera/waybeam/etc/aalink.conf`; compare before overwriting
  either one.
- `install.sh` - the older re-deploy after a reflash (socat on the VRX, local
  `aalink.star6e`). The current path is wyvern-ai-osd `tools/build/build_aalink.sh`
  then `tools/deploy/cam_link_deploy.sh`.

Build, from the repository root (WSL, `~/tc/armv7-eabihf--glibc--stable-2023.11-1`):
`arm-buildroot-linux-gnueabihf-gcc -O2 -march=armv7-a -mfpu=neon -mfloat-abi=hard -o wyvern/aalink.star6e aalink.c -lpthread -lm`

## What this fork changes vs upstream sickgreg/aalink
1. **Recovery fix** - on a rising signal, `rssi_local` tracks the fast EMA
   instead of the lagging slow one, so the link climbs back after a dip.
2. **Best-antenna RSSI** from the ground feeds the bitrate ladder.
3. **ROI degrade step** - as the bitrate falls, `fpv.roiCenter` shrinks
   (0.90 -> 0.20) alongside the roiQp ramp, through waybeam's `/live/set`
   (RAM only: `/set` would rewrite `/etc/waybeam.json` on every step).
4. **Ground video loss** (`<rssi>,<tag>,loss=<permille>` on :12345) counts once
   per adaptation window.
5. **`API_CMD` as a program** (`/usr/bin/gslink %d`): it was taken for a URL
   path, so no bitrate was ever applied before.
6. **Thermal step-down that cools**: at `HIGH_TEMP` the SENSOR's rate drops
   (`setfps` on `mi_sensor0`: 30, then 20/10/5 fps), with waybeam's live rate
   set to match; waybeam's `video0.fps` alone only decimates and cooled
   nothing (116 C on the bench). The bitrate meanwhile holds `THERMAL_KBPS`
   (upstream: 100 kbps, unflyable). The rate restored afterwards is gslink's
   `FPS_FULL`, not `/etc/waybeam.json`, which a persisting API call can
   overwrite with a lowered rate.
7. **Idempotent resync** - the bitrate is re-affirmed for the current
   (mcs, width) on a stable link, so a failed command self-corrects.

## The ladder (wyvern `aalink.conf`)
```
PR=100,600,1200,3250,6500,13000,19500,26000,39000,52000,58500,65000
THROUGHPUT_PCT=30
```
From MCS 0 up, `PR` holds the real 802.11n one-stream rates at 20 MHz; the
bitrate is `PR[mcs] x width/20 x THROUGHPUT_PCT`: 3900 kbps at MCS 0, 7800,
11700, 15600, 23400 at MCS 4 (40 MHz). gslink then clamps it (1500..30000)
and ramps rises. The old table (11000 for every MCS above 0) at 90 % ran the
link saturated: flight 2026-10-28 17:03 lost 0.7 % in the medium zone, one
second at 55 %; at 30 % the same zone lost nothing.

Other settings the Wyvern runs:
```
MCS_SOURCE=lowest
MAX_MCS_40_EU=4            # (and _AU/_BU/_CU = 4)
SIMPLE_ROIQP=-17
ENABLE_THRESH_ADAPT=0
HIGH_TEMP=95
THERMAL_KBPS=1500
API_CMD=/usr/bin/gslink %d
```

## Firmware package (not done - future)
To survive a reflash automatically, add an OpenIPC buildroot package that
installs `aalink` + a default `aalink.conf`, then rebuild the camera firmware.
