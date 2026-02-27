# aalink

`aalink` is a lightweight adaptive link controller for Wi-Fi video links.
It monitors link quality (RSSI/PUBQ, optionally SNR), adjusts MCS/tx power, and can update an encoder bitrate endpoint while publishing OSD status.

## Repository contents

- `aalink30c.c` — main daemon source.
- `aalink.conf` — example runtime configuration.

## Build (cross-compile for Star6E hard-float)

Use an ARM hard-float cross toolchain (for example `arm-linux-gnueabihf`) and build a static binary for the Star6E target:

```bash
export CROSS_COMPILE=arm-linux-gnueabihf
${CROSS_COMPILE}-gcc \
  -O2 -Wall -Wextra \
  -march=armv7-a -mfpu=neon -mfloat-abi=hard \
  -o aalink aalink30c.c \
  -lpthread -lm
```

If your Star6E SDK provides a different triplet, replace `arm-linux-gnueabihf` with the SDK-provided prefix.

## Install

```bash
cp aalink /usr/bin/aalink
chmod 755 /usr/bin/aalink
```

## Run

```bash
/usr/bin/aalink
```

The daemon reads `/etc/aalink.conf` and writes status messages to `/tmp/MSPOSD.msg` (and optional extended data to `/tmp/aalink_ext.msg`).

## Configuration highlights

`aalink.conf` supports many tuning parameters; common ones include:

- `IFACE` — wireless interface to control (default `wlan0`).
- `PING_DEST` / `ACTIVE_IP` — peer endpoints used for link probing.
- `API_CMD` — command template used to set encoder bitrate (`%d` placeholder required).
- `PR` — bitrate table mapped to virtual MCS `-4..7`.
- `THRESH_*` / `MAX_MCS_*` / `PWR_*` — profile-specific thresholds, caps, and power limits.
- `OSD_LEVEL`, `OSD_PARAMS`, `OSD_SCALE`, `EXTERNAL_OSD` — OSD behavior.
- `USE_CHEAP_PROC`, `USE_SNR`, `SNR_MULTI` — rtl88x2eu proc-hook optimizations.

## Notes

- This project is Linux-oriented and expects nl80211/procfs interfaces provided by supported Wi-Fi drivers.
- Start from `aalink.conf`, then tune thresholds and power tables for your region/hardware profile.
