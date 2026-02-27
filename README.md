# aalink

`aalink` is a lightweight adaptive link controller for Wi-Fi video links.
It monitors link quality (RSSI/PUBQ, optionally SNR), adjusts MCS/tx power, and can update an encoder bitrate endpoint while publishing OSD status.

## Repository contents

- `aalink30c.c` — main daemon source.
- `aalink.conf` — example runtime configuration.

## Build

Use GCC with pthread and math support:

```bash
gcc -O2 -Wall -Wextra -o aalink aalink30c.c -lpthread -lm
```

## Install

```bash
sudo install -m 0755 aalink /usr/local/bin/aalink
sudo install -m 0644 aalink.conf /etc/aalink.conf
```

## Run

```bash
sudo /usr/local/bin/aalink
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
