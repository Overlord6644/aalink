#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <netinet/ip_icmp.h>
#include <signal.h>
#include <errno.h>
#include <ctype.h>
#include <limits.h>
#include <stdint.h>
#include <math.h>
#include <time.h>
#include <stdbool.h>
#include <pthread.h>
#include <stdatomic.h>

#include <net/if.h>
#ifdef __linux__
#include <linux/netlink.h>
#include <linux/genetlink.h>
#include <linux/nl80211.h>
#endif


// --- RSSI / Ping worker ---
static pthread_t rssi_thread;
static pthread_mutex_t rssi_mutex = PTHREAD_MUTEX_INITIALIZER;
static int latest_rssi_local = -1;
static int rssi_sample_interval_ms = 33;
// --- RSSI history for trend detection ---
#define RSSI_HISTORY_LEN 16   // ~0.5s at 33ms sampling
static int rssi_history[RSSI_HISTORY_LEN];
static int rssi_hist_idx = 0;
static int rssi_hist_count = 0;
// --- RSSI EMA globals ---
static double ema_fast = -1, ema_slow = -1;
static const double alpha_fast = 0.4;  // fast reacts quickly
static const double alpha_slow = 0.1;  // slow is a stable baseline
static int latest_rssi_fast = -1;
static int latest_rssi_slow = -1;
static float predict_multi = 1.0;

// --- PUBQ worker ---
static pthread_t pubq_thread;
static pthread_mutex_t pubq_mutex = PTHREAD_MUTEX_INITIALIZER;
static atomic_int pubq_stop = 0;

typedef struct {
    int raw;
    float ema_fast;
    float ema_slow;
    int min_100ms;
    int max_100ms;
    uint64_t ts_ms;

    /* derived / helper fields filled by pubq_worker */
    float norm_fast;   // ema_fast / PUBQ_NOMINAL
    float norm_slow;   // ema_slow  / PUBQ_NOMINAL
    float gap;         // norm_slow - norm_fast

    /* detection flags filled by pubq_worker (main reads them) */
    int hard_confirmed;            // debounced "hard" collapse
    int collapse_confirmed;        // sustained collapse (hold period passed)
    uint64_t collapse_confirmed_ts_ms;
} pubq_stats_t;

static pubq_stats_t latest_pubq = {0};
static float fps_cut_aggressiveness = 1.0f;

// --- MCS async worker ---
typedef struct {
    int mcs, width, cur_mcs;
    float scale;
} mcs_job_t;

static pthread_t mcs_thread;
static pthread_mutex_t mcs_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t mcs_cond = PTHREAD_COND_INITIALIZER;
static atomic_int mcs_stop = 0, mcs_job_pending = 0;
static mcs_job_t mcs_job;


/* Track the actual hardware-applied MCS separately from the target mcs */
static atomic_int hw_mcs = 0;
/* Track the last successfully applied TX power setpoint (mBm). */
static atomic_int hw_txpower_set_mbm = ATOMIC_VAR_INIT(INT_MIN);
/* --- Virtual MCS support: allow -4..7 (12 entries) --- */
#define MCS_MIN   (-4)
#define MCS_MAX   (7)
#define MCS_COUNT (MCS_MAX - MCS_MIN + 1) /* 12 */
/* Map MCS value (-4..7) to array index (0..11) */
#define MCS_IDX(m) ((int)((m) - MCS_MIN))

#define CONF_PATH        "/etc/aalink.conf"
#define MSG_PATH         "/tmp/MSPOSD.msg"
#define EXT_MSG_PATH     "/tmp/aalink_ext.msg"
#define PROC_NET_DIR     "/proc/net"
#define PROC_NET_DIR        "/proc/net"
#define RATE_SUBPATH_FMT    "/%s/%s/rate_ctl"
#define TRX_PUBQ_FMT "/%s/%s/trx_info"
#define TRX_SUBPATH_FMT     "/%s/%s/trx_info_debug"
#define AP_INFO_SUBPATH_FMT "/%s/%s/ap_info"

// Cheap per-metric proc hooks (rtl88x2eu)
#define RSSI_A_SUBPATH_FMT      "/%s/%s/rssi_a"
#define RSSI_B_SUBPATH_FMT      "/%s/%s/rssi_b"
#define SNR_A_SUBPATH_FMT       "/%s/%s/snr_a"
#define SNR_B_SUBPATH_FMT       "/%s/%s/snr_b"
#define PUBQ_FREE_SUBPATH_FMT   "/%s/%s/pubq_free_page"
#define RX_INFO_MSG_SUBPATH_FMT "/%s/%s/rx_info_msg"

static char iface[32] = "wlan0", ping_dest[64] = "192.168.0.10", active_ip[64] = "192.168.0.10", active_mac[18] = "";
static char api_cmd[256] = "/api/v1/set?video0.bitrate=%d", osd_params[64] = "&L31&F20";
static char mcs_source[16] = "downlink", driver_name[NAME_MAX] = {0};
static char rate_path[256], proc_trx_path[256], proc_ap_info_path[256], proc_pubq_path[256];
// Cheap proc hook paths (conditionally populated)
static char proc_rssi_a_path[256], proc_rssi_b_path[256];
static char proc_snr_a_path[256], proc_snr_b_path[256];
static char proc_pubq_free_path[256];
static char proc_rx_info_msg_path[256];

static int pr[12] = {0}, thresh_eu[12] = {0}, thresh_au[12] = {0}, thresh_bu[12] = {0}, thresh_cu[12] = {0}, pct_thresh[12] = {0};
static float mcs0_sub_low_th = 0.75f;        /* kept for compatibility, no longer used in logic */
static float mcs0_sub_very_low_th = 0.5f;    /* kept for compatibility, no longer used in logic */
static int pwr_eu[MCS_COUNT] = {0}, pwr_au[MCS_COUNT] = {0}, pwr_bu[MCS_COUNT] = {0}, pwr_cu[MCS_COUNT] = {0};
static float scale_tx_power = 1.0f;
static double scale_rssi = 0; 

static int max_mcs = 7, max_mcs_40 = 5;
static int max_mcs_eu = 5, max_mcs_au = 5, max_mcs_bu = 5, max_mcs_cu = 5;
static int max_mcs_40_eu = 4, max_mcs_40_au = 4, max_mcs_40_bu = 4, max_mcs_40_cu = 4;
static int max_mcs_80 = 2;
static int max_mcs_80_eu = 2, max_mcs_80_au = 2, max_mcs_80_bu = 2, max_mcs_80_cu = 2;

static int thresh_shift = 0;
int antenna_mismatch = 0;

static int osd_kb = 0, udp_port = 12345, ping_sock = -1, refresh_cnt = 50, status_cnt = 5, udp_timeout_loops = 50;
static float H_pct = 0.10f;
static int upward_confidence_loops = 3;
int up_cooldown_ms = 2000;
static pid_t ping_pid = -1;
static uint16_t ping_seq;
static int osd_level = 1, external_osd = 1, reduced_power_mode = 0, show_signal_bars = 0, show_tx_power = 1, verbose = 0;
static uint64_t reduce_heat_cool_since_ms = 0; // used to require cool temp for a duration before exiting REDUCING_HEAT
static float throughput_pct = 50, osd_scale = 1.0f;
static struct sockaddr_in ping_dst;
static volatile sig_atomic_t reload_requested = 0;
static int PUBQ_NOMINAL = 211, PUBQ_NOMINAL_AU = 214, PUBQ_NOMINAL_EU = 1753, PUBQ_NOMINAL_BU = 211, PUBQ_NOMINAL_CU = 1753;
static int enable_fps_cut = 0;
int osd_cuts = 0;
static int osd_q = -1;
static int osd_colour_mod = 3;
static int avg_ant = 1;
static int high_temp = 95;
static int simple_roiQp = 0;
static int use_snr_mode = 0;
static float snr_multi = 1.5f;
static int latest_snr_norm = -1;
static int use_cheap_proc = 0;  // opt-in via conf

// Cheap hook availability flags
static int cheap_rssi_avail = 0;          // rssi_a/b files exist
static int cheap_pubq_avail = 0;          // pubq_free_page file exists
static int cheap_pubq_reports_used = 1;   // semantics match legacy PUBQ parsing

// --- Threshold auto-adaptation ---
static int enable_thresh_autoadapt = 0;
static float thresh_adapt_gain = 0.5f;
static int max_thresh_shift = 15;
static float dynamic_shift = 0.0f;
static float  pubq_f_short = 1.0f, pubq_f_long = 1.0f;

// Quadratic dip tracking
static float dip_score = 0.0f;
static uint64_t dip_last_window_ms = 0;
static uint64_t pubq_last_change_ms = 0;
static uint64_t pubq_emerg_last_step_ms = 0;
static uint64_t pubq_emerg_clear_req_ms = 0;
static uint64_t pubq_normal_grace_ms = 0;

/* FPS cut timing tunables (worker uses these) */
#define FPS_CUT_HOLD_MS          200   /* sustained collapse hold before confirming (ms) */
#define FPS_CUT_HARD_WINDOW_MS    30   /* window to require two hard hits (ms) */
#define FPS_CUT_HARD_COOLDOWN_MS 300   /* how long hard_confirmed remains valid (ms) */

#define VPRINT(level, fmt, ...) \
    do { if (verbose >= level) fprintf(stderr, fmt "\n", ##__VA_ARGS__); } while (0)


// --- nl80211 via raw generic netlink (no libnl dependency) ---
// Replaces:
//   - iw dev <iface> info                -> NL80211_CMD_GET_INTERFACE
//   - iw dev <iface> station dump        -> NL80211_CMD_GET_STATION (dump; first station)
//   - iw dev <iface> set txpower fixed X -> NL80211_CMD_SET_WIPHY (fixed)
//
// This keeps a persistent NETLINK_GENERIC socket and resolves the nl80211 family id
// via GENL control messages.
//
// If nl80211 is unavailable or a call fails, helpers return negative errno-style codes;
// callers generally tolerate "unknown" results.

#ifndef DISABLE_NL80211_RAW

typedef struct {
    int fd;
    uint16_t nl80211_id;
    int ifindex;
    char ifname[32];
    uint32_t seq;
    uint32_t pid;
} nl_ctx_t;

static nl_ctx_t g_nl = {0};

#ifndef NLA_ALIGNTO
#define NLA_ALIGNTO 4
#endif
#ifndef NLA_ALIGN
#define NLA_ALIGN(len) (((len) + NLA_ALIGNTO - 1) & ~(NLA_ALIGNTO - 1))
#endif
#ifndef NLA_HDRLEN
#define NLA_HDRLEN ((int)NLA_ALIGN(sizeof(struct nlattr)))
#endif
#ifndef GENL_HDRLEN
#define GENL_HDRLEN ((int)NLMSG_ALIGN(sizeof(struct genlmsghdr)))
#endif
#ifndef NLA_TYPE_MASK
#define NLA_TYPE_MASK 0x3fff
#endif

static int freq_to_channel_mhz(int f) {
    if (f == 2484) return 14;
    if (f >= 2412 && f <= 2472) return (f - 2407) / 5;
    if (f >= 5000 && f <= 5900) return (f - 5000) / 5;
    if (f >= 5955 && f <= 7115) return (f - 5950) / 5; // 6 GHz rough
    return 0;
}

static int chanwidth_to_mhz(uint32_t w) {
    switch ((enum nl80211_chan_width)w) {
        case NL80211_CHAN_WIDTH_20:
        case NL80211_CHAN_WIDTH_20_NOHT: return 20;
        case NL80211_CHAN_WIDTH_40:      return 40;
        case NL80211_CHAN_WIDTH_80:      return 80;
        case NL80211_CHAN_WIDTH_80P80:   return 80;
        case NL80211_CHAN_WIDTH_160:     return 160;
        case NL80211_CHAN_WIDTH_5:       return 5;
        case NL80211_CHAN_WIDTH_10:      return 10;
        default: return 0;
    }
}

static inline void *nla_data(const struct nlattr *nla) {
    return (void *)((char *)nla + NLA_HDRLEN);
}
static inline int nla_len(const struct nlattr *nla) {
    return (int)nla->nla_len - NLA_HDRLEN;
}
static inline int nla_ok(const struct nlattr *nla, int rem) {
    return rem >= (int)sizeof(*nla) && nla->nla_len >= sizeof(*nla) && (int)nla->nla_len <= rem;
}
static inline struct nlattr *nla_next(struct nlattr *nla, int *rem) {
    int tot = NLA_ALIGN(nla->nla_len);
    *rem -= tot;
    return (struct nlattr *)((char *)nla + tot);
}
static inline uint32_t nla_get_u32(const struct nlattr *nla) {
    uint32_t v;
    memcpy(&v, nla_data(nla), sizeof(v));
    return v;
}
static inline uint16_t nla_get_u16(const struct nlattr *nla) {
    uint16_t v;
    memcpy(&v, nla_data(nla), sizeof(v));
    return v;
}
static inline uint8_t nla_get_u8(const struct nlattr *nla) {
    uint8_t v;
    memcpy(&v, nla_data(nla), sizeof(v));
    return v;
}

static int nla_put(struct nlmsghdr *nlh, size_t maxlen, int type, const void *data, size_t datalen) {
    size_t off = NLMSG_ALIGN(nlh->nlmsg_len);
    size_t need = NLA_HDRLEN + datalen;
    size_t tot = NLA_ALIGN(need);
    if (off + tot > maxlen) return -ENOBUFS;

    struct nlattr *a = (struct nlattr *)((char *)nlh + off);
    a->nla_type = (uint16_t)type;
    a->nla_len  = (uint16_t)need;
    if (datalen) memcpy(nla_data(a), data, datalen);
    if (tot > need) memset((char *)a + need, 0, tot - need);

    nlh->nlmsg_len = (uint32_t)(off + tot);
    return 0;
}
static int nla_put_u32(struct nlmsghdr *nlh, size_t maxlen, int type, uint32_t v) {
    return nla_put(nlh, maxlen, type, &v, sizeof(v));
}
static int nla_put_string(struct nlmsghdr *nlh, size_t maxlen, int type, const char *s) {
    return nla_put(nlh, maxlen, type, s, strlen(s) + 1);
}

static void nla_parse_flat(struct nlattr **tb, int maxtype, void *data, int len) {
    if (maxtype < 0) return;
    memset(tb, 0, (maxtype + 1) * sizeof(tb[0]));
    int rem = len;
    struct nlattr *a = (struct nlattr *)data;
    while (nla_ok(a, rem)) {
        int t = (int)(a->nla_type & NLA_TYPE_MASK);
        if (t >= 0 && t <= maxtype) tb[t] = a;
        a = nla_next(a, &rem);
    }
}

static void nl_close_ctx(void) {
    if (g_nl.fd > 0) close(g_nl.fd);
    memset(&g_nl, 0, sizeof(g_nl));
}

static int genl_resolve_family_id(int fd, uint16_t *out_id, uint32_t *seq_io, uint32_t pid, const char *family) {
    char buf[256];
    memset(buf, 0, sizeof(buf));
    struct nlmsghdr *nlh = (struct nlmsghdr *)buf;
    nlh->nlmsg_len   = NLMSG_LENGTH(sizeof(struct genlmsghdr));
    nlh->nlmsg_type  = GENL_ID_CTRL;
    nlh->nlmsg_flags = NLM_F_REQUEST;
    nlh->nlmsg_seq   = ++(*seq_io);
    nlh->nlmsg_pid   = pid;

    struct genlmsghdr *gh = (struct genlmsghdr *)NLMSG_DATA(nlh);
    gh->cmd = CTRL_CMD_GETFAMILY;
    gh->version = 1;

    int r = nla_put_string(nlh, sizeof(buf), CTRL_ATTR_FAMILY_NAME, family);
    if (r < 0) return r;

    struct sockaddr_nl nladdr = {0};
    nladdr.nl_family = AF_NETLINK;

    struct iovec iov = { .iov_base = nlh, .iov_len = nlh->nlmsg_len };
    struct msghdr msg = {0};
    msg.msg_name = &nladdr;
    msg.msg_namelen = sizeof(nladdr);
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    if (sendmsg(fd, &msg, 0) < 0) return -errno;

    // Receive one response (not a dump)
    char rbuf[8192];
    while (1) {
        ssize_t n = recv(fd, rbuf, sizeof(rbuf), 0);
        if (n < 0) return -errno;
        struct nlmsghdr *h;
        for (h = (struct nlmsghdr *)rbuf; NLMSG_OK(h, (unsigned)n); h = NLMSG_NEXT(h, n)) {
            if (h->nlmsg_seq != nlh->nlmsg_seq) continue;

            if (h->nlmsg_type == NLMSG_ERROR) {
                struct nlmsgerr *e = (struct nlmsgerr *)NLMSG_DATA(h);
                return e->error ? e->error : 0;
            }

            struct genlmsghdr *rgh = (struct genlmsghdr *)NLMSG_DATA(h);
            int alen = (int)(h->nlmsg_len - NLMSG_HDRLEN - GENL_HDRLEN);
            if (alen < 0) continue;

            struct nlattr *tb[CTRL_ATTR_MAX + 1];
            void *adata = (char *)rgh + GENL_HDRLEN;
            nla_parse_flat(tb, CTRL_ATTR_MAX, adata, alen);

            if (tb[CTRL_ATTR_FAMILY_ID]) {
                *out_id = nla_get_u16(tb[CTRL_ATTR_FAMILY_ID]);
                return 0;
            }
        }
    }
}

static int nl_open_ctx(const char *ifname) {
    int ifindex = if_nametoindex(ifname);
    if (!ifindex) return -ENODEV;

    int fd = socket(AF_NETLINK, SOCK_RAW | SOCK_CLOEXEC, NETLINK_GENERIC);
    if (fd < 0 && errno == EINVAL) fd = socket(AF_NETLINK, SOCK_RAW, NETLINK_GENERIC);
    if (fd < 0) return -errno;

    struct sockaddr_nl local = {0};
    local.nl_family = AF_NETLINK;
    local.nl_pid = (uint32_t)getpid();
    if (bind(fd, (struct sockaddr *)&local, sizeof(local)) < 0) {
        int e = -errno;
        close(fd);
        return e;
    }

    socklen_t sl = sizeof(local);
    if (getsockname(fd, (struct sockaddr *)&local, &sl) == 0)
        local.nl_pid = local.nl_pid ? local.nl_pid : (uint32_t)getpid();

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 250 * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    uint16_t fam = 0;
    uint32_t seq = 0;
    int r = genl_resolve_family_id(fd, &fam, &seq, local.nl_pid, "nl80211");
    if (r < 0 || fam == 0) {
        close(fd);
        return (r < 0) ? r : -ENOENT;
    }

    nl_close_ctx();
    g_nl.fd = fd;
    g_nl.nl80211_id = fam;
    g_nl.ifindex = ifindex;
    g_nl.seq = seq;
    g_nl.pid = local.nl_pid;
    strncpy(g_nl.ifname, ifname, sizeof(g_nl.ifname)-1);
    return 0;
}

static int nl_ensure_ctx(void) {
    if (g_nl.fd <= 0 || strncmp(g_nl.ifname, iface, sizeof(g_nl.ifname)) != 0)
        return nl_open_ctx(iface);
    return 0;
}

typedef int (*nl_payload_cb)(struct genlmsghdr *gh, int attr_len, void *arg);

static int nl_send_and_recv(uint8_t cmd, uint16_t flags, struct nlmsghdr *req, size_t reqlen,
                            nl_payload_cb cb, void *cb_arg, int is_dump, int want_ack) {
    struct sockaddr_nl nladdr = {0};
    nladdr.nl_family = AF_NETLINK;

    struct iovec iov = { .iov_base = req, .iov_len = req->nlmsg_len };
    struct msghdr msg = {0};
    msg.msg_name = &nladdr;
    msg.msg_namelen = sizeof(nladdr);
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    if (sendmsg(g_nl.fd, &msg, 0) < 0) return -errno;

    int got_ack = 0;
    int done = 0;
    int loops = 0;

    char rbuf[8192];
    while (!done && loops++ < 8) {
        ssize_t n = recv(g_nl.fd, rbuf, sizeof(rbuf), 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            return -errno;
        }

        struct nlmsghdr *h;
        ssize_t rem = n;
        for (h = (struct nlmsghdr *)rbuf; NLMSG_OK(h, (unsigned)rem); h = NLMSG_NEXT(h, rem)) {
            if (h->nlmsg_seq != req->nlmsg_seq) continue;

            if (h->nlmsg_type == NLMSG_ERROR) {
                struct nlmsgerr *e = (struct nlmsgerr *)NLMSG_DATA(h);
                if (e->error) return e->error; // already negative errno
                got_ack = 1;
                if (want_ack && !is_dump) return 0;
                continue;
            }

            if (h->nlmsg_type == NLMSG_DONE) {
                done = 1;
                break;
            }

            // payload
            struct genlmsghdr *gh = (struct genlmsghdr *)NLMSG_DATA(h);
            int alen = (int)(h->nlmsg_len - NLMSG_HDRLEN - GENL_HDRLEN);
            if (alen < 0) continue;

            if (cb) {
                int r = cb(gh, alen, cb_arg);
                if (r < 0) return r;
            }

            // Non-dump "get" requests typically return a single payload.
            if (!is_dump && !want_ack) return 0;
        }
    }

    if (want_ack && got_ack) return 0;
    // For dump requests, caller decides success based on filled output; returning 0 is fine.
    return 0;
}

/* GET_INTERFACE: channel/width */
typedef struct {
    int freq_mhz;
    int channel;
    int width_mhz;
    int have_freq;
    int have_width;
} nl_ifinfo_t;

static int nl_ifinfo_cb(struct genlmsghdr *gh, int attr_len, void *arg) {
    nl_ifinfo_t *o = (nl_ifinfo_t *)arg;
    struct nlattr *tb[NL80211_ATTR_MAX + 1];
    void *adata = (char *)gh + GENL_HDRLEN;
    nla_parse_flat(tb, NL80211_ATTR_MAX, adata, attr_len);

    if (tb[NL80211_ATTR_WIPHY_FREQ]) {
        o->freq_mhz = (int)nla_get_u32(tb[NL80211_ATTR_WIPHY_FREQ]);
        o->channel = freq_to_channel_mhz(o->freq_mhz);
        o->have_freq = 1;
    }
    if (tb[NL80211_ATTR_CHANNEL_WIDTH]) {
        o->width_mhz = chanwidth_to_mhz(nla_get_u32(tb[NL80211_ATTR_CHANNEL_WIDTH]));
        o->have_width = 1;
    } else if (tb[NL80211_ATTR_WIPHY_CHANNEL_TYPE]) {
        enum nl80211_channel_type ct = (enum nl80211_channel_type)nla_get_u32(tb[NL80211_ATTR_WIPHY_CHANNEL_TYPE]);
        o->width_mhz = (ct == NL80211_CHAN_HT40MINUS || ct == NL80211_CHAN_HT40PLUS) ? 40 : 20;
        o->have_width = 1;
    }
    return 0;
}

static int nl_get_channel_width(int *channel, int *width_mhz) {
    if (nl_ensure_ctx() < 0) return -1;

    nl_ifinfo_t o;
    memset(&o, 0, sizeof(o));

    char buf[256];
    memset(buf, 0, sizeof(buf));
    struct nlmsghdr *nlh = (struct nlmsghdr *)buf;
    nlh->nlmsg_len   = NLMSG_LENGTH(sizeof(struct genlmsghdr));
    nlh->nlmsg_type  = g_nl.nl80211_id;
    nlh->nlmsg_flags = NLM_F_REQUEST;
    nlh->nlmsg_seq   = ++g_nl.seq;
    nlh->nlmsg_pid   = g_nl.pid;

    struct genlmsghdr *gh = (struct genlmsghdr *)NLMSG_DATA(nlh);
    gh->cmd = NL80211_CMD_GET_INTERFACE;
    gh->version = 0;

    if (nla_put_u32(nlh, sizeof(buf), NL80211_ATTR_IFINDEX, (uint32_t)g_nl.ifindex) < 0)
        return -ENOBUFS;

    int r = nl_send_and_recv(NL80211_CMD_GET_INTERFACE, 0, nlh, sizeof(buf), nl_ifinfo_cb, &o, 0, 0);
    if (r < 0) return r;

    if (channel) *channel = o.channel;
    if (width_mhz) *width_mhz = o.width_mhz;
    return 0;
}


/* GET_STATION (dump): first signal */
typedef struct {
    int got;
    int signal_dbm;
} nl_sta_sig_t;

static int nl_sta_cb(struct genlmsghdr *gh, int attr_len, void *arg) {
    nl_sta_sig_t *o = (nl_sta_sig_t *)arg;

    struct nlattr *tb[NL80211_ATTR_MAX + 1];
    void *adata = (char *)gh + GENL_HDRLEN;
    nla_parse_flat(tb, NL80211_ATTR_MAX, adata, attr_len);
    if (!tb[NL80211_ATTR_STA_INFO]) return 0;

    struct nlattr *sinfo[NL80211_STA_INFO_MAX + 1];
    void *sdata = nla_data(tb[NL80211_ATTR_STA_INFO]);
    int slen = nla_len(tb[NL80211_ATTR_STA_INFO]);
    nla_parse_flat(sinfo, NL80211_STA_INFO_MAX, sdata, slen);

    if (sinfo[NL80211_STA_INFO_SIGNAL_AVG]) {
        int8_t s = (int8_t)nla_get_u8(sinfo[NL80211_STA_INFO_SIGNAL_AVG]);
        o->signal_dbm = (int)s;
        o->got = 1;
    } else if (sinfo[NL80211_STA_INFO_SIGNAL]) {
        int8_t s = (int8_t)nla_get_u8(sinfo[NL80211_STA_INFO_SIGNAL]);
        o->signal_dbm = (int)s;
        o->got = 1;
    }
    return 0;
}

static int nl_get_first_station_signal_dbm(int *signal_dbm) {
    if (nl_ensure_ctx() < 0) return -1;

    nl_sta_sig_t o;
    memset(&o, 0, sizeof(o));

    char buf[256];
    memset(buf, 0, sizeof(buf));
    struct nlmsghdr *nlh = (struct nlmsghdr *)buf;
    nlh->nlmsg_len   = NLMSG_LENGTH(sizeof(struct genlmsghdr));
    nlh->nlmsg_type  = g_nl.nl80211_id;
    nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_DUMP;
    nlh->nlmsg_seq   = ++g_nl.seq;
    nlh->nlmsg_pid   = g_nl.pid;

    struct genlmsghdr *gh = (struct genlmsghdr *)NLMSG_DATA(nlh);
    gh->cmd = NL80211_CMD_GET_STATION;
    gh->version = 0;

    if (nla_put_u32(nlh, sizeof(buf), NL80211_ATTR_IFINDEX, (uint32_t)g_nl.ifindex) < 0)
        return -ENOBUFS;

    int r = nl_send_and_recv(NL80211_CMD_GET_STATION, 0, nlh, sizeof(buf), nl_sta_cb, &o, 1, 0);
    if (r < 0) return r;
    if (!o.got) return -ENOENT;

    if (signal_dbm) *signal_dbm = o.signal_dbm;
    return 0;
}

/* SET_WIPHY: txpower fixed (mBm) */
static int nl_set_txpower_fixed_mbm(int mbm) {
    if (nl_ensure_ctx() < 0) return -1;

    char buf[256];
    memset(buf, 0, sizeof(buf));
    struct nlmsghdr *nlh = (struct nlmsghdr *)buf;
    nlh->nlmsg_len   = NLMSG_LENGTH(sizeof(struct genlmsghdr));
    nlh->nlmsg_type  = g_nl.nl80211_id;
    nlh->nlmsg_flags = NLM_F_REQUEST | NLM_F_ACK;
    nlh->nlmsg_seq   = ++g_nl.seq;
    nlh->nlmsg_pid   = g_nl.pid;

    struct genlmsghdr *gh = (struct genlmsghdr *)NLMSG_DATA(nlh);
    gh->cmd = NL80211_CMD_SET_WIPHY;
    gh->version = 0;

    if (nla_put_u32(nlh, sizeof(buf), NL80211_ATTR_IFINDEX, (uint32_t)g_nl.ifindex) < 0) return -ENOBUFS;
    if (nla_put_u32(nlh, sizeof(buf), NL80211_ATTR_WIPHY_TX_POWER_SETTING, (uint32_t)NL80211_TX_POWER_FIXED) < 0) return -ENOBUFS;
    if (nla_put_u32(nlh, sizeof(buf), NL80211_ATTR_WIPHY_TX_POWER_LEVEL, (uint32_t)mbm) < 0) return -ENOBUFS;

    return nl_send_and_recv(NL80211_CMD_SET_WIPHY, 0, nlh, sizeof(buf), NULL, NULL, 0, 1);
}

#else
// If you ever need to compile without nl80211 headers, define DISABLE_NL80211_RAW.
// In that case, these helpers become stubs and will return "not available".
static int nl_get_channel_width(int *channel, int *width_mhz) { (void)channel; (void)width_mhz; return -1; }
static int nl_get_first_station_signal_dbm(int *signal_dbm) { (void)signal_dbm; return -1; }
static int nl_set_txpower_fixed_mbm(int mbm) { (void)mbm; return -1; }
#endif

static int parse_csv(const char *s, int out[MCS_COUNT]) {
    char *dup = strdup(s), *tok, *p = dup;
    int i = 0;
    while ((tok = strsep(&p, ",")) && i < MCS_COUNT)
        out[i++] = atoi(tok);
    free(dup);
    return (i == MCS_COUNT);
}

static inline uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000ULL;
}

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

// --- Local HTTP helper (replaces external fetch) ---
// Performs a GET to 127.0.0.1:80. If body_out is NULL, the response body is ignored.
static int http_get_local(const char *path, char *body_out, size_t body_cap) {
    if (!path || !*path) return -1;

    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0 && errno == EINVAL) fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    fcntl(fd, F_SETFD, FD_CLOEXEC);

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 200 * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(80);
    sa.sin_addr.s_addr = htonl(0x7F000001); // 127.0.0.1

    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        close(fd);
        return -1;
    }

    char req[384];
    int rn = snprintf(req, sizeof(req),
                      "GET %s HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
                      path);
    if (rn <= 0 || rn >= (int)sizeof(req)) { close(fd); return -1; }

    int off = 0;
    while (off < rn) {
        int s = send(fd, req + off, rn - off, MSG_NOSIGNAL);
        if (s <= 0) { close(fd); return -1; }
        off += s;
    }

    if (!body_out || body_cap == 0) { close(fd); return 0; }

    char resp[16384];
    int total = 0;
    while (total < (int)sizeof(resp) - 1) {
        int r = recv(fd, resp + total, (int)sizeof(resp) - 1 - total, 0);
        if (r == 0) break;
        if (r < 0) {
            if (errno == EINTR) continue;
            break;
        }
        total += r;
    }
    close(fd);
    resp[total] = 0;

    char *body = strstr(resp, "\r\n\r\n");
    if (!body) return -1;
    body += 4;

    size_t blen = strlen(body);
    if (blen >= body_cap) blen = body_cap - 1;
    memcpy(body_out, body, blen);
    body_out[blen] = 0;
    return (int)blen;
}

// Extract a local HTTP path from a string (supports either "/path..." or "http://127.0.0.1/path...").
// Returns 0 on success.
static int extract_http_local_path(const char *s, char *out, size_t out_cap) {
    if (!s || !out || out_cap < 2) return -1;
    out[0] = 0;

    if (s[0] == '/') {
        strncpy(out, s, out_cap - 1);
        out[out_cap - 1] = 0;
        return 0;
    }

    const char *p = strstr(s, "http://127.0.0.1");
    const char *p2 = strstr(s, "http://localhost");
    if (!p || (p2 && p2 < p)) p = p2;
    if (!p) return -1;

    const char *slash = strchr(p + 7, '/'); // after "http://"
    if (!slash) return -1;

    const char *end = slash;
    while (*end && *end != '"' && *end != '\'' && *end != ' ' && *end != '\t' &&
           *end != '\r' && *end != '\n')
        end++;

    size_t n = (size_t)(end - slash);
    if (n == 0) return -1;
    if (n >= out_cap) n = out_cap - 1;
    memcpy(out, slash, n);
    out[n] = 0;
    return 0;
}

// Run API command: if it's a local URL/path (including legacy command strings), use http_get_local(); otherwise fallback to system().
static int run_local_api_cmd(const char *cmd_or_path) {
    char path[256];
    if (extract_http_local_path(cmd_or_path, path, sizeof(path)) == 0) {
        return http_get_local(path, NULL, 0);
    }
    return system(cmd_or_path);
}

static int majestic_get_video0_fps(int *out_fps) {
    if (!out_fps) return -1;
    char body[16384];
    int n = http_get_local("/api/v1/config.json", body, sizeof(body));
    if (n <= 0) return -1;

    const char *p = strstr(body, "\"video0\"");
    if (!p) return -1;
    p = strchr(p, '{');
    if (!p) return -1;

    const char *f = strstr(p, "\"fps\"");
    if (!f) return -1;
    f = strchr(f, ':');
    if (!f) return -1;
    f++;
    while (*f && !isdigit((unsigned char)*f) && *f != '-') f++;
    if (!*f) return -1;

    *out_fps = atoi(f);
    return 0;
}


// Read FPS from /etc/majestic.yaml (stable config) via yaml-cli/cli.
// This avoids relying on /api/v1/config.json which can reflect transient runtime state.
static int majestic_get_video0_fps_from_yaml(int *out_fps) {
    if (!out_fps) return -1;

    const char *cmds[] = {
        "cli -g .video0.fps 2>/dev/null",
        "yaml-cli -i /etc/majestic.yaml -g .video0.fps 2>/dev/null",
        "yaml-cli -g .video0.fps -i /etc/majestic.yaml 2>/dev/null",
        NULL
    };

    char buf[256];
    for (int i = 0; cmds[i]; i++) {
        FILE *f = popen(cmds[i], "r");
        if (!f) continue;
        if (!fgets(buf, sizeof(buf), f)) { pclose(f); continue; }
        pclose(f);

        char *p = buf;
        while (*p && !isdigit((unsigned char)*p) && *p != '-') p++;
        if (!*p) continue;

        int v = atoi(p);
        if (v > 0 && v < 1000) { *out_fps = v; return 0; }
    }

    return -1;
}


// SigmaStar / mi_sensor procfs FPS setter
// Example: echo 'setfps 0 5' > /proc/mi_modules/mi_sensor/mi_sensor0
#define MI_SENSOR_FPS_PATH "/proc/mi_modules/mi_sensor/mi_sensor0"
static int mi_sensor_set_fps(int fps) {
    if (fps <= 0) return -1;
    int fd = open(MI_SENSOR_FPS_PATH, O_WRONLY | O_CLOEXEC);
    if (fd < 0) return -1;

    char cmd[64];
    int n = snprintf(cmd, sizeof(cmd), "setfps 0 %d\n", fps);
    if (n <= 0 || n >= (int)sizeof(cmd)) { close(fd); return -1; }
    ssize_t wr = write(fd, cmd, (size_t)n);
    close(fd);
    return (wr == (ssize_t)n) ? 0 : -1;
}

// Re-apply current configured FPS from /etc/majestic.yaml (same path used by thermal restore).
static int reapply_fps_from_majestic_yaml(const char *reason) {
    int file_fps = 0;
    if (majestic_get_video0_fps_from_yaml(&file_fps) != 0 || file_fps <= 0) return -1;
    if (mi_sensor_set_fps(file_fps) != 0) return -1;
    if (reason && *reason) VPRINT(1, "Re-applied FPS from majestic.yaml -> %d (%s)", file_fps, reason);
    else VPRINT(1, "Re-applied FPS from majestic.yaml -> %d", file_fps);
    return file_fps;
}

// --- Cached /proc reader (avoid open/close churn in tight loops) ---
#define PROC_CACHE_SLOTS 16
typedef struct {
    char path[PATH_MAX];
    int  fd;
} proc_cache_entry_t;

static proc_cache_entry_t proc_cache[PROC_CACHE_SLOTS];
static pthread_mutex_t proc_cache_mutex = PTHREAD_MUTEX_INITIALIZER;

static void proc_cache_reset(void) {
    pthread_mutex_lock(&proc_cache_mutex);
    for (int i = 0; i < PROC_CACHE_SLOTS; i++) {
        if (proc_cache[i].path[0] && proc_cache[i].fd >= 0) close(proc_cache[i].fd);
        proc_cache[i].fd = -1;
        proc_cache[i].path[0] = '\0';
    }
    pthread_mutex_unlock(&proc_cache_mutex);
}

static int proc_cache_get_fd_locked(const char *path, int *out_idx) {
    if (out_idx) *out_idx = -1;
    if (!path || !*path) return -1;

    // Fast path: existing entry
    for (int i = 0; i < PROC_CACHE_SLOTS; i++) {
        if (proc_cache[i].path[0] && strcmp(proc_cache[i].path, path) == 0) {
            if (out_idx) *out_idx = i;
            return proc_cache[i].fd;
        }
    }

    // Find an empty slot, else evict slot 0 (small fixed set; keep it simple)
    int slot = -1;
    for (int i = 0; i < PROC_CACHE_SLOTS; i++) {
        if (!proc_cache[i].path[0]) { slot = i; break; }
    }
    if (slot < 0) slot = 0;

    if (proc_cache[slot].path[0] && proc_cache[slot].fd >= 0) close(proc_cache[slot].fd);
    proc_cache[slot].fd = -1;
    proc_cache[slot].path[0] = '\0';

    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;

    proc_cache[slot].fd = fd;
    strncpy(proc_cache[slot].path, path, sizeof(proc_cache[slot].path) - 1);
    proc_cache[slot].path[sizeof(proc_cache[slot].path) - 1] = '\0';
    if (out_idx) *out_idx = slot;
    return fd;
}

static ssize_t proc_read_cached(const char *path, char *buf, size_t bufsz) {
    if (!buf || bufsz < 2) return -1;

    pthread_mutex_lock(&proc_cache_mutex);
    int idx = -1;
    int fd = proc_cache_get_fd_locked(path, &idx);
    ssize_t n = -1;
    if (fd >= 0) {
        n = pread(fd, buf, bufsz - 1, 0);
        if (n <= 0) {
            // Driver/module might have been unloaded; force a reopen next time.
            if (idx >= 0) {
                if (proc_cache[idx].path[0] && proc_cache[idx].fd >= 0) close(proc_cache[idx].fd);
                proc_cache[idx].fd = -1;
                proc_cache[idx].path[0] = '\0';
            }
        }
    }
    pthread_mutex_unlock(&proc_cache_mutex);

    if (n <= 0) return -1;
    buf[n] = '\0';
    return n;
}

static int proc_read_int_cached(const char *path, int *out) {
    if (!out) return -1;
    char buf[96];
    if (proc_read_cached(path, buf, sizeof(buf)) < 0) return -1;

    char *p = buf;
    while (*p && !isdigit((unsigned char)*p) && *p != '-') p++;
    if (!*p) return -1;

    char *end = NULL;
    long v = strtol(p, &end, 10);
    if (end == p) return -1;
    *out = (int)v;
    return 0;
}

static ssize_t proc_read_cached_dyn(const char *path, char **pbuf, size_t *pcap,
                                   size_t min_cap, size_t max_cap)
{
    if (!pbuf || !pcap) return -1;
    if (min_cap < 1024) min_cap = 1024;
    if (max_cap < min_cap) max_cap = min_cap;

    if (*pcap < min_cap) {
        char *nb = (char *)realloc(*pbuf, min_cap);
        if (!nb) return -1;
        *pbuf = nb;
        *pcap = min_cap;
    }

    for (;;) {
        ssize_t n = proc_read_cached(path, *pbuf, *pcap);
        if (n < 0) return -1;
        if ((size_t)n < *pcap - 1) return n;
        if (*pcap >= max_cap) return n;

        size_t next = *pcap * 2;
        if (next > max_cap) next = max_cap;
        char *nb = (char *)realloc(*pbuf, next);
        if (!nb) return n;
        *pbuf = nb;
        *pcap = next;
    }
}

static void cleanup(void) {
    if (ping_sock >= 0) close(ping_sock), ping_sock = -1;
    if (mcs_thread) {
        mcs_stop = 1;
        pthread_cond_signal(&mcs_cond);
        pthread_join(mcs_thread, NULL);
        mcs_thread = 0;
    }
	if (rssi_thread) {
		mcs_stop = 1;
		pthread_join(rssi_thread, NULL);
		rssi_thread = 0;
	}
	if (pubq_thread) {
		pubq_stop = 1;
		pthread_join(pubq_thread, NULL);
		pubq_thread = 0;
	}
	proc_cache_reset();
}

static void handle_signal(int sig) {
    if (sig == SIGHUP) {
        VPRINT(1, "Caught SIGHUP, reload requested");
        reload_requested = 1;
        return;
    }

    VPRINT(1, "Caught signal %d, cleaning up", sig);
    cleanup();
    exit(0);
}

int update_active_mac_from_ip(void) {
    FILE *f = fopen("/proc/net/arp", "r"); if (!f) return -1;
    char l[256], ip[64], mac[18]; fgets(l, sizeof(l), f);
    while (fgets(l, sizeof(l), f))
        if (sscanf(l, "%63s %*s %*s %17s", ip, mac) == 2 && !strcmp(ip, active_ip)) {
            strncpy(active_mac, mac, sizeof(active_mac)-1);
            active_mac[sizeof(active_mac)-1] = 0; fclose(f); return 0;
        }
    fclose(f); active_mac[0] = 0; return -1;
}

static void load_config(void) {
    FILE *f = fopen(CONF_PATH, "r");
    if (!f) { perror("fopen"); exit(1); }
    char line[256];
    int have_thresh_cu = 0, have_pwr_cu = 0;
    int have_max_mcs_cu = 0, have_max_mcs_40_cu = 0, have_max_mcs_80_cu = 0;
    int have_pubq_nominal_cu = 0;
    while (fgets(line, sizeof(line), f)) {
        char *p = line, *eq;
        while (isspace(*p)) p++;
        if (*p == '#' || !*p) continue;
        if (!(eq = strchr(p, '='))) continue;
        *eq = '\0'; char *key = p, *val = eq + 1;
        val[strcspn(val, "\r\n")] = 0;

        if (!strcmp(key, "IFACE")) strncpy(iface, val, sizeof(iface)-1);
        else if (!strcmp(key, "UDP_PORT")) udp_port = atoi(val);
        else if (!strcmp(key, "PING_DEST")) strncpy(ping_dest, val, sizeof(ping_dest) - 1);
		else if (!strcmp(key, "ACTIVE_IP")) strncpy(active_ip, val, sizeof(active_ip) - 1);
		else if (!strcmp(key, "API_CMD")) strncpy(api_cmd, val, sizeof(api_cmd)-1);
        else if (!strcmp(key, "PR")) { if (!parse_csv(val, pr)) goto bad; }
		else if (!strcmp(key, "H")) H_pct = atof(val) / 100.0f;
        else if (!strcmp(key, "UP_COOLDOWN_MS")) up_cooldown_ms = atoi(val);
        else if (!strcmp(key, "UPWARD_CONFIDENCE") || !strcmp(key, "UPWARD_CONFIDENCE_LOOPS")) upward_confidence_loops = atoi(val);
		else if (!strcmp(key, "REFRESH_CNT")) refresh_cnt = atoi(val);
        else if (!strcmp(key, "MAX_MCS")) max_mcs = atoi(val);
        else if (!strcmp(key, "STATUS_CNT")) status_cnt = atoi(val);
        else if (!strcmp(key, "UDP_TIMEOUT_LOOPS")) udp_timeout_loops = atoi(val);
        else if (!strcmp(key, "OSD_PARAMS")) strncpy(osd_params, val, sizeof(osd_params)-1);
        else if (!strcmp(key, "THRESH_EU")) { if (!parse_csv(val, thresh_eu)) goto bad; }
        else if (!strcmp(key, "THRESH_AU")) { if (!parse_csv(val, thresh_au)) goto bad; }
        else if (!strcmp(key, "THRESH_BU")) { if (!parse_csv(val, thresh_bu)) goto bad; }
        else if (!strcmp(key, "THRESH_CU")) { have_thresh_cu = 1; if (!parse_csv(val, thresh_cu)) goto bad; }
		else if (!strcmp(key, "MCS0_SUB_LOW_TH")) mcs0_sub_low_th = atof(val);
		else if (!strcmp(key, "MCS0_SUB_VERY_LOW_TH")) mcs0_sub_very_low_th = atof(val);
        else if (!strcmp(key, "MAX_MCS_EU")) max_mcs_eu = atoi(val);
        else if (!strcmp(key, "MAX_MCS_AU")) max_mcs_au = atoi(val);
        else if (!strcmp(key, "MAX_MCS_BU")) max_mcs_bu = atoi(val);
        else if (!strcmp(key, "MAX_MCS_CU")) { have_max_mcs_cu = 1; max_mcs_cu = atoi(val); }
        else if (!strcmp(key, "MAX_MCS_40_EU")) max_mcs_40_eu = atoi(val);
        else if (!strcmp(key, "MAX_MCS_40_AU")) max_mcs_40_au = atoi(val);
        else if (!strcmp(key, "MAX_MCS_40_BU")) max_mcs_40_bu = atoi(val);
        else if (!strcmp(key, "MAX_MCS_40_CU")) { have_max_mcs_40_cu = 1; max_mcs_40_cu = atoi(val); }
		else if (!strcmp(key, "MAX_MCS_80_EU")) max_mcs_80_eu = atoi(val);
		else if (!strcmp(key, "MAX_MCS_80_AU")) max_mcs_80_au = atoi(val);
		else if (!strcmp(key, "MAX_MCS_80_BU")) max_mcs_80_bu = atoi(val);
		else if (!strcmp(key, "MAX_MCS_80_CU")) { have_max_mcs_80_cu = 1; max_mcs_80_cu = atoi(val); }

		else if (!strcmp(key, "THRESH_SHIFT")) thresh_shift = atoi(val);
		else if (!strcmp(key, "PWR_EU")) { if (!parse_csv(val, pwr_eu)) goto bad; }
		else if (!strcmp(key, "PWR_AU")) { if (!parse_csv(val, pwr_au)) goto bad; }
		else if (!strcmp(key, "PWR_BU")) { if (!parse_csv(val, pwr_bu)) goto bad; }
		else if (!strcmp(key, "PWR_CU")) { have_pwr_cu = 1; if (!parse_csv(val, pwr_cu)) goto bad; }
		else if (!strcmp(key, "SCALE_TX_POWER")) scale_tx_power = atof(val);

        else if (!strcmp(key, "OSD_LEVEL")) osd_level = atoi(val);
		else if (!strcmp(key, "EXTERNAL_OSD")) external_osd = atoi(val);
		else if (!strcmp(key, "OSD_SCALE")) osd_scale = atof(val);
		else if (!strcmp(key, "HIGH_TEMP")) high_temp = atoi(val);
		else if (!strcmp(key, "REDUCED_POWER_MODE")) reduced_power_mode = atoi(val);
		else if (!strcmp(key, "MCS_SOURCE")) strncpy(mcs_source, val, sizeof(mcs_source) - 1);
		else if (!strcmp(key, "SHOW_SIGNAL_BARS")) show_signal_bars = !strcasecmp(val, "true");
		else if (!strcmp(key, "SHOW_TX_POWER")) show_tx_power = !strcasecmp(val, "true");
		else if (!strcmp(key, "THROUGHPUT_PCT")) throughput_pct = atof(val);

		else if (!strcmp(key, "ENABLE_FPS_CUT")) enable_fps_cut = atoi(val);
		else if (!strcmp(key, "PUBQ_NOMINAL_AU")) PUBQ_NOMINAL_AU = atoi(val);
		else if (!strcmp(key, "PUBQ_NOMINAL_EU")) PUBQ_NOMINAL_EU = atoi(val);
		else if (!strcmp(key, "PUBQ_NOMINAL_BU")) PUBQ_NOMINAL_BU = atoi(val);
		else if (!strcmp(key, "PUBQ_NOMINAL_CU")) { have_pubq_nominal_cu = 1; PUBQ_NOMINAL_CU = atoi(val); }
        else if (!strcmp(key, "ENABLE_THRESH_ADAPT")) enable_thresh_autoadapt = atoi(val);
        else if (!strcmp(key, "THRESH_ADAPT_GAIN")) thresh_adapt_gain = atof(val);
        else if (!strcmp(key, "MAX_THRESH_SHIFT")) max_thresh_shift = atoi(val);
		else if (!strcmp(key, "RSSI_SAMPLE_INTERVAL_MS")) rssi_sample_interval_ms = atoi(val);
		else if (!strcmp(key, "SCALE_RSSI")) scale_rssi = atof(val);
		else if (!strcmp(key, "PREDICT_MULTI")) predict_multi = atof(val);
		else if (!strcmp(key, "FPS_CUT_AGGRESSIVENESS")) fps_cut_aggressiveness = atof(val);
		else if (!strcmp(key, "SIMPLE_ROIQP")) simple_roiQp = atoi(val);
        else if (!strcmp(key, "USE_SNR")) use_snr_mode = atoi(val);
        else if (!strcmp(key, "SNR_MULTI")) snr_multi = atof(val);
        else if (!strcmp(key, "USE_CHEAP_PROC")) use_cheap_proc = atoi(val);

        continue;
    bad:
        fprintf(stderr, "Bad config line: %s", line);
        exit(1);
    }
    // Defaults for rtl88x2cu config: mirror EU unless explicitly provided
    if (!have_thresh_cu) memcpy(thresh_cu, thresh_eu, sizeof(thresh_eu));
    if (!have_pwr_cu)    memcpy(pwr_cu,   pwr_eu,   sizeof(pwr_eu));
    if (!have_max_mcs_cu)      max_mcs_cu      = max_mcs_eu;
    if (!have_max_mcs_40_cu)   max_mcs_40_cu   = max_mcs_40_eu;
    if (!have_max_mcs_80_cu)   max_mcs_80_cu   = max_mcs_80_eu;
    if (!have_pubq_nominal_cu) PUBQ_NOMINAL_CU = PUBQ_NOMINAL_EU;

    fclose(f);
}

static void apply_config(void) {
    update_active_mac_from_ip();

	if (strcmp(driver_name, "rtl88xxau") == 0 || strcmp(driver_name, "rtl8812au") == 0) {
		max_mcs      = max_mcs_au;
		max_mcs_40   = max_mcs_40_au;
		max_mcs_80   = max_mcs_80_au;
		memcpy(pct_thresh, thresh_au, sizeof(thresh_au));
		PUBQ_NOMINAL = PUBQ_NOMINAL_AU;
	} else if (strcmp(driver_name, "rtl8733bu") == 0) {
		max_mcs      = max_mcs_bu;
		max_mcs_40   = max_mcs_40_bu;
		max_mcs_80   = max_mcs_80_bu;
		memcpy(pct_thresh, thresh_bu, sizeof(thresh_bu));
		PUBQ_NOMINAL = PUBQ_NOMINAL_BU;
	} else if (strcmp(driver_name, "rtl88x2eu") == 0) {
		max_mcs      = max_mcs_eu;
		max_mcs_40   = max_mcs_40_eu;
		max_mcs_80   = max_mcs_80_eu;
		memcpy(pct_thresh, thresh_eu, sizeof(thresh_eu));
		PUBQ_NOMINAL = PUBQ_NOMINAL_EU;
	} else if (strcmp(driver_name, "rtl88x2cu") == 0) {
		max_mcs      = max_mcs_cu;
		max_mcs_40   = max_mcs_40_cu;
		max_mcs_80   = max_mcs_80_cu;
		memcpy(pct_thresh, thresh_cu, sizeof(thresh_cu));
		PUBQ_NOMINAL = PUBQ_NOMINAL_CU;
	} else {
		VPRINT(1, "Note:  Did not match driver name with thresholds in config. Using AU defaults");
		max_mcs      = max_mcs_au;
		max_mcs_40   = max_mcs_40_au;
		max_mcs_80   = max_mcs_80_au;
		memcpy(pct_thresh, thresh_au, sizeof(thresh_au));
		PUBQ_NOMINAL = PUBQ_NOMINAL_AU;
	}

	if (thresh_shift != 0) {
		for (int i = 0; i < MCS_COUNT; i++) {
			float frac = i / (float)(MCS_COUNT - 1);
			int shifted = pct_thresh[i] + (int)(thresh_shift * frac);

			if (shifted < 1) shifted = 1;
			if (shifted > 100) shifted = 100;
			pct_thresh[i] = shifted;
		}

		// enforce monotonic increase
		for (int i = 1; i < MCS_COUNT; i++) {
			if (pct_thresh[i] <= pct_thresh[i - 1]) {
				pct_thresh[i] = pct_thresh[i - 1] + 1;
				if (pct_thresh[i] > 100) pct_thresh[i] = 100;
			}
		}
	}
	
	if (scale_tx_power > 0.0f && scale_tx_power < 1.0f) {
		for (int i = 0; i < sizeof(pwr_eu)/sizeof(pwr_eu[0]); i++) pwr_eu[i] = (int)(pwr_eu[i] * scale_tx_power);
		for (int i = 0; i < sizeof(pwr_au)/sizeof(pwr_au[0]); i++) pwr_au[i] = (int)(pwr_au[i] * scale_tx_power);
		for (int i = 0; i < sizeof(pwr_bu)/sizeof(pwr_bu[0]); i++) pwr_bu[i] = (int)(pwr_bu[i] * scale_tx_power);
		for (int i = 0; i < sizeof(pwr_cu)/sizeof(pwr_cu[0]); i++) pwr_cu[i] = (int)(pwr_cu[i] * scale_tx_power);
	}
	
    if (verbose >= 1) {
        char s[128] = {0}, *p = s;
        for (int i = 0; i < MCS_COUNT; i++)
            p += snprintf(p, sizeof(s) - (p - s), "%d%s", pct_thresh[i], (i < (MCS_COUNT-1)) ? "," : "");
		VPRINT(1, "MCS cap @ 20MHz = %d, @ 40MHz = %d, @ 80MHz = %d", max_mcs, max_mcs_40, max_mcs_80);
        VPRINT(1, "Thresholds: %s (shift=%d)", s, thresh_shift);
        VPRINT(1, "PUBQ nominal = %d", PUBQ_NOMINAL);
    }
	
}

static uint16_t cksum(const void *data, int len) {
    uint32_t sum = 0;
    const uint16_t *ptr = data;
    while (len > 1) sum += *ptr++, len -= 2;
    if (len == 1) sum += *(const uint8_t *)ptr;
    sum = (sum >> 16) + (sum & 0xFFFF);
    return ~((sum >> 16) + sum);
}

void start_ping(void) {
    if (ping_sock >= 0) close(ping_sock);
    ping_sock = socket(AF_INET, SOCK_RAW, IPPROTO_ICMP);
    if (ping_sock < 0) return;

    memset(&ping_dst, 0, sizeof(ping_dst));
    ping_dst.sin_family = AF_INET;
    if (inet_pton(AF_INET, ping_dest, &ping_dst.sin_addr) != 1) {
        close(ping_sock); ping_sock = -1;
    }
    ping_seq = 0;
}

void send_ping(void) {
    if (ping_sock < 0) return;
    struct {
        struct icmphdr hdr;
        char data[56];
    } pkt = {0};

    pkt.hdr.type = ICMP_ECHO;
    pkt.hdr.un.echo.id = htons(getpid());
    pkt.hdr.un.echo.sequence = htons(ping_seq++);
    pkt.hdr.checksum = cksum(&pkt, sizeof(pkt));

    sendto(ping_sock, &pkt, sizeof(pkt), 0,
           (struct sockaddr *)&ping_dst, sizeof(ping_dst));
}

static int find_proc_path(const char *subfmt, char *out, size_t len, const char *iface) {
    DIR *d = opendir(PROC_NET_DIR);
    if (!d) return -1;

    size_t pfx = strlen("/%s/%s/");
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_type!=DT_DIR || e->d_name[0]=='.') continue;
        char cand[PATH_MAX];
        snprintf(cand, sizeof(cand),
                 PROC_NET_DIR"/%s/%s/%s",
                 e->d_name, iface, subfmt + pfx);
        if (!access(cand, R_OK)) {
            strncpy(driver_name, e->d_name, NAME_MAX-1);
            driver_name[NAME_MAX-1] = '\0';
            strncpy(out, cand, len-1);
            out[len-1] = '\0';
            closedir(d);
            return 0;
        }
    }
    closedir(d);
    return -1;
}

static void reset_cheap_hook_state(void) {
    proc_rssi_a_path[0] = '\0';
    proc_rssi_b_path[0] = '\0';
    proc_snr_a_path[0] = '\0';
    proc_snr_b_path[0] = '\0';
    proc_pubq_free_path[0] = '\0';
    proc_rx_info_msg_path[0] = '\0';
    cheap_rssi_avail = 0;
    cheap_pubq_avail = 0;
    cheap_pubq_reports_used = 1;
}

static void detect_cheap_hooks(void) {
    reset_cheap_hook_state();

    if (!(use_cheap_proc && (!strcmp(driver_name, "rtl88x2eu") || !strcmp(driver_name, "rtl88x2cu"))))
        return;

    int got_a = (find_proc_path(RSSI_A_SUBPATH_FMT, proc_rssi_a_path, sizeof(proc_rssi_a_path), iface) == 0);
    int got_b = (find_proc_path(RSSI_B_SUBPATH_FMT, proc_rssi_b_path, sizeof(proc_rssi_b_path), iface) == 0);
    (void)find_proc_path(SNR_A_SUBPATH_FMT, proc_snr_a_path, sizeof(proc_snr_a_path), iface);
    (void)find_proc_path(SNR_B_SUBPATH_FMT, proc_snr_b_path, sizeof(proc_snr_b_path), iface);

    cheap_rssi_avail = got_a;
    if (got_a) VPRINT(1, "Detected RSSI_A hook: %s", proc_rssi_a_path);
    if (got_b) VPRINT(1, "Detected RSSI_B hook: %s", proc_rssi_b_path);
    if (proc_snr_a_path[0]) VPRINT(1, "Detected SNR_A hook: %s", proc_snr_a_path);
    if (proc_snr_b_path[0]) VPRINT(1, "Detected SNR_B hook: %s", proc_snr_b_path);
    if (cheap_rssi_avail)
        VPRINT(1, "Cheap RSSI hooks ready (rssi_a%s)", got_b ? " + rssi_b" : " only");

    if (find_proc_path(RX_INFO_MSG_SUBPATH_FMT, proc_rx_info_msg_path, sizeof(proc_rx_info_msg_path), iface) == 0) {
        VPRINT(1, "Detected RX_INFO_MSG hook: %s", proc_rx_info_msg_path);
        int fd = open(proc_rx_info_msg_path, O_WRONLY | O_CLOEXEC);
        if (fd >= 0) {
            const char one[] = "1\n";
            ssize_t __w = write(fd, one, sizeof(one) - 1);
            (void)__w;
            close(fd);
            VPRINT(1, "Enabled rx_info_msg for SNR at %s", proc_rx_info_msg_path);
        }
    }

    if (find_proc_path(PUBQ_FREE_SUBPATH_FMT, proc_pubq_free_path, sizeof(proc_pubq_free_path), iface) == 0) {
        cheap_pubq_avail = 1;
        cheap_pubq_reports_used = 1;
        VPRINT(1, "Detected PUBQ_FREE_PAGE hook: %s", proc_pubq_free_path);
    }
}

static void summarize_sources(void) {
    if (!strcmp(driver_name, "rtl88xxau")) {
        VPRINT(1, "Using RSSI source: nl80211 get station (AU)");
    } else if ((!strcmp(driver_name, "rtl88x2eu") || !strcmp(driver_name, "rtl88x2cu")) && use_cheap_proc && cheap_rssi_avail) {
        if (proc_rssi_b_path[0])
            VPRINT(1, "Using RSSI source: %s and %s", proc_rssi_a_path, proc_rssi_b_path);
        else
            VPRINT(1, "Using RSSI source: %s", proc_rssi_a_path);
    } else if (proc_trx_path[0]) {
        VPRINT(1, "Using RSSI source: %s (trx_info_debug)", proc_trx_path);
    }

    if ((!strcmp(driver_name, "rtl88x2eu") || !strcmp(driver_name, "rtl88x2cu")) && use_cheap_proc) {
        if (proc_snr_a_path[0] || proc_snr_b_path[0]) {
            if (proc_snr_a_path[0] && proc_snr_b_path[0])
                VPRINT(1, "Using SNR source: %s and %s", proc_snr_a_path, proc_snr_b_path);
            else if (proc_snr_a_path[0])
                VPRINT(1, "Using SNR source: %s", proc_snr_a_path);
            else
                VPRINT(1, "Using SNR source: %s", proc_snr_b_path);
        } else {
            VPRINT(1, "Using SNR source: none");
        }
    }

    if (cheap_pubq_avail) {
        VPRINT(1, "Using PUBQ source: %s (free pages)", proc_pubq_free_path);
    } else if (proc_pubq_path[0]) {
        VPRINT(1, "Using PUBQ source: %s (used pages)", proc_pubq_path);
    } else {
        VPRINT(1, "Using PUBQ source: none");
    }
}

static int get_ap_info(int *width, int *channel) {
    if (proc_ap_info_path[0]) {
        char buf[2048];
        if (proc_read_cached(proc_ap_info_path, buf, sizeof(buf)) > 0) {
            char *line = buf;
            while (line && *line) {
                char *next = strchr(line, '\n');
                if (next) *next++ = '\0';

                int ch, bw;
                if (sscanf(line, "cur_channel=%d, cur_bwmode=%d", &ch, &bw) == 2) {
                    *channel = ch;
                    *width = (bw == 0) ? 20 : (bw == 1) ? 40 : (bw == 2) ? 80 : 0;
                    VPRINT(2, "Used procfs ap_info to read width %d and channel %d", *width, *channel);
                    return 0;
                }
                line = next;
            }
        }
    }

    *width = 0; *channel = 0;
    int ch = 0, w = 0;
    if (nl_get_channel_width(&ch, &w) == 0) {
        *channel = ch;
        *width = w;
        VPRINT(2, "Used nl80211 raw to read width %d and channel %d", *width, *channel);
    }
    return 0; // unknown values are acceptable
}


static int read_pubq(const char *path) {
    if (!path || !*path) return -1;
    char buf[4096];
    if (proc_read_cached(path, buf, sizeof(buf)) < 0) return -1;

    int pubq = -1;
    char *p = strstr(buf, "PUBQ:");
    if (p) (void)sscanf(p, "PUBQ: %d", &pubq);
    return pubq;
}

// Read a simple integer from path
static int read_int_file(const char *path, int *out) {
    if (!path || !*path) return -1;
    return proc_read_int_cached(path, out);
}

static int read_station_dump_rssi(void) {
    int dbm_i = 0;
    int pct = -1;
    if (nl_get_first_station_signal_dbm(&dbm_i) == 0) {
        float dbm = (float)dbm_i;
        pct = (int)(dbm + 90.5f);
        if (pct < 0) pct = 0;
        else if (pct > 100) pct = 100;
        VPRINT(2, "AU RSSI: %.1f dBm → %d%% (nl80211 raw)", dbm, pct);
    }
    return pct;
}

static int read_proc_rssi(void) {
	static int mismatch_ms = 0;
	static int good_ms = 0;
    if (!proc_trx_path[0]) return -1;

    static char *big = NULL;
    static size_t cap = 0;

    if (proc_read_cached_dyn(proc_trx_path, &big, &cap, 8192, 262144) < 0)
        return -1;

    int sta_count = 0;
    int rssi_single = -1;
    int rssi_multi = -1;
    int in_active = 0;
    char mac[18];

    char *line = big;
    while (line && *line) {
        char *next = strchr(line, '\n');
        if (next) *next++ = '\0';

        if (sscanf(line, "============ STA [%17[^]]]  ===================", mac) == 1) {
            sta_count++;
            in_active = (active_mac[0] && strcmp(mac, active_mac) == 0);
            line = next;
            continue;
        }

        if (sta_count > 0) {
            if (in_active) {
                int r;
                if (sscanf(line, "rssi : %d", &r) == 1) { rssi_multi = r; break; }
                if (strstr(line, "============")) in_active = 0;
            }
        } else {
            int a, b;
            if (sscanf(line, "%*[^=]=%*s rssi_a = %d(%%), rssi_b = %d(%%)", &a, &b) == 2) {
                const int dt = (rssi_sample_interval_ms > 0) ? rssi_sample_interval_ms : 33;

                /* Latch after ~10s mismatch; clear after ~20s good (time-based, not sample-count based) */
                if (abs(a - b) >= 20) {
                    good_ms = 0;
                    mismatch_ms += dt;
                    if (mismatch_ms >= 10000) {
                        antenna_mismatch = 1;
                        mismatch_ms = 10000;
                    }
                } else {
                    mismatch_ms = 0;
                    if (antenna_mismatch) {
                        good_ms += dt;
                        if (good_ms >= 20000) {
                            antenna_mismatch = 0;
                            good_ms = 0;
                        }
                    } else {
                        good_ms = 0;
                    }
                }
                rssi_single = avg_ant ? (a + b) / 2 : (a > b ? a : b);
            } else if (sscanf(line, "%*[^=]=%*s rssi_a = %d(%%)", &a) == 1) {
                rssi_single = a;
            }
        }

        line = next;
    }

    if (sta_count > 1)
        return rssi_multi;
    return (rssi_single >= 0) ? rssi_single : rssi_multi;
}

// Cheap RSSI reader for rtl88x2eu using per-antenna proc files
static int read_proc_rssi_cheap(int *snr_norm_out) {
	static int mismatch_ms = 0;
	static int good_ms = 0;
    int a = -1, b = -1;

    if (!cheap_rssi_avail)
        return -1;

    // rssi_a is required for cheap mode; rssi_b may be absent on some setups
    if (read_int_file(proc_rssi_a_path, &a) < 0)
        return -1;
    (void)read_int_file(proc_rssi_b_path, &b); // best-effort

    if (b >= 0) {
        const int dt = (rssi_sample_interval_ms > 0) ? rssi_sample_interval_ms : 33;

        /* Latch after ~10s mismatch; clear after ~20s good (time-based, not sample-count based) */
        if (abs(a - b) >= 20) {
            good_ms = 0;
            mismatch_ms += dt;
            if (mismatch_ms >= 10000) {
                antenna_mismatch = 1;
                mismatch_ms = 10000;
            }
        } else {
            mismatch_ms = 0;
            if (antenna_mismatch) {
                good_ms += dt;
                if (good_ms >= 20000) {
                    antenna_mismatch = 0;
                    good_ms = 0;
                }
            } else {
                good_ms = 0;
            }
        }
    }

    // Optionally read SNR values (not used yet)
    int snr_a = -1, snr_b = -1;
    int snr_norm = -1;
    (void)read_int_file(proc_snr_a_path, &snr_a);
    (void)read_int_file(proc_snr_b_path, &snr_b);
    if (snr_a >= 0 || snr_b >= 0) {
        VPRINT(3, "SNR_A=%d SNR_B=%d", snr_a, snr_b);

        int snr_combined = -1;
        if (snr_a >= 0 && snr_b >= 0)
            snr_combined = avg_ant ? (snr_a + snr_b) / 2 : (snr_a > snr_b ? snr_a : snr_b);
        else if (snr_a >= 0)
            snr_combined = snr_a;
        else if (snr_b >= 0)
            snr_combined = snr_b;

        if (snr_combined >= 0) {
            float scaled = snr_combined * snr_multi;
            if (scaled < 0.0f) scaled = 0.0f;
            if (scaled > 100.0f) scaled = 100.0f;
            snr_norm = (int)lrintf(scaled);
        }
    }

    if (snr_norm_out)
        *snr_norm_out = snr_norm;

    if (b >= 0)
        return avg_ant ? (a + b) / 2 : (a > b ? a : b);
    return a;
}

static inline int apply_log_scale(int pct) {
    if (pct < 0) pct = 0;
    if (pct > 100) pct = 100;
    if (scale_rssi > 0.0 && pct > 0 && pct < 100)
        pct = (int)lrint(log1p(scale_rssi * (pct / 100.0)) / log1p(scale_rssi) * 100.0);
    return pct;
}

static int combine_with_snr(int rssi_pct, int snr_pct) {
    latest_snr_norm = snr_pct;

    if (use_snr_mode <= 0)
        return rssi_pct;

    if (strcmp(driver_name, "rtl88x2eu") != 0 && strcmp(driver_name, "rtl88x2cu") != 0)
        return rssi_pct;

    if (snr_pct < 0)
        return rssi_pct;

    if (use_snr_mode == 1)
        return snr_pct;

    if (use_snr_mode == 2) {
        if (rssi_pct >= 0)
            return (snr_pct + rssi_pct) / 2;
        return snr_pct;
    }

    return rssi_pct;
}

static int rssi_read(void) {
    int snr_norm = -1;
    int pct;
    if (!strcmp(driver_name, "rtl88xxau")) {
        pct = read_station_dump_rssi();
    } else if ((!strcmp(driver_name, "rtl88x2eu") || !strcmp(driver_name, "rtl88x2cu")) && use_cheap_proc && cheap_rssi_avail) {
        pct = read_proc_rssi_cheap(&snr_norm);
    } else {
        pct = read_proc_rssi();
    }

    int combined = combine_with_snr(pct, snr_norm);
    return apply_log_scale(combined);
}

static void *rssi_worker(void *arg) {
    (void)arg;
    while (!mcs_stop) {
        int local = rssi_read();

        if (local >= 0) {
            if (ema_fast < 0) {
                ema_fast = ema_slow = local;
            }
            ema_fast = alpha_fast * local + (1.0 - alpha_fast) * ema_fast;
            ema_slow = alpha_slow * local + (1.0 - alpha_slow) * ema_slow;
        }

        pthread_mutex_lock(&rssi_mutex);
        if (local >= 0 && local <= 100)
            latest_rssi_local = local;
        latest_rssi_fast = (int)lrint(ema_fast);
        latest_rssi_slow = (int)lrint(ema_slow);
        rssi_history[rssi_hist_idx] = local;
        rssi_hist_idx = (rssi_hist_idx + 1) % RSSI_HISTORY_LEN;
        if (rssi_hist_count < RSSI_HISTORY_LEN) rssi_hist_count++;
        pthread_mutex_unlock(&rssi_mutex);

        send_ping();
        usleep(rssi_sample_interval_ms * 1000);
    }
    return NULL;
}

// get most recent RSSI sample
static int rssi_last(void) {
    if (rssi_hist_count == 0) return -1;
    int idx = (rssi_hist_idx - 1 + RSSI_HISTORY_LEN) % RSSI_HISTORY_LEN;
    return rssi_history[idx];
}

static int set_mcs_width(int m, int width_mhz) {
    char hex[16];
    unsigned base = (width_mhz == 80) ? 0x02C : 0x00C;
    snprintf(hex, sizeof(hex), "0x%X\n", base + m);
    int fd = open(rate_path, O_WRONLY | O_CLOEXEC);
    if (fd < 0) { perror("open rate_ctl"); return -1; }
    ssize_t w = write(fd, hex, strlen(hex));
    close(fd);
    return (w == (ssize_t)strlen(hex) ? 0 : -1);
}

static void set_power(int mcs) {
    int val = 0;
    if (strcmp(driver_name, "rtl88x2eu") == 0)       val = pwr_eu[MCS_IDX(mcs)];
    else if (strcmp(driver_name, "rtl88x2cu") == 0)  val = pwr_cu[MCS_IDX(mcs)];
    else if (strcmp(driver_name, "rtl88xxau") == 0 ||
             strcmp(driver_name, "rtl8812au") == 0)  val = pwr_au[MCS_IDX(mcs)];
    else if (strcmp(driver_name, "rtl8733bu") == 0)  val = pwr_bu[MCS_IDX(mcs)];

    if (val != 0) {
        int r = nl_set_txpower_fixed_mbm(val);
        if (r == 0) {
            VPRINT(2, "Set TX power to %d for MCS %d (nl80211 raw)", val, mcs);
            if (atomic_load(&hw_txpower_set_mbm) != val) atomic_store(&hw_txpower_set_mbm, val);
        } else {
            VPRINT(1, "TX power set failed for %d mBm (MCS %d) via nl80211 raw: %d (%s)", val, mcs, r, strerror(-r));
        }
    } else {
        VPRINT(2, "No TX power change for MCS %d", mcs);
    }
}

static void apply_roiQp(int qp) {
    static int p = 999;
    if (qp == p) return;
    char path[96];
    snprintf(path, sizeof(path), "/api/v1/set?fpv.roiQp=%d", qp);
    run_local_api_cmd(path);
    p = qp;
}

// Vary ROI QP delta between:
//  - > 11000 kbps : 0 (ROI off)
//  - 11000 kbps   : 25% of SIMPLE_ROIQP
//  - 2000 kbps    : 100% of SIMPLE_ROIQP
static inline int calc_roiQp_from_kbps(int kbps)
{
    const int hi = 11000;
    const int lo = 2000;
    const int span = (hi - lo); // 9000

    if (kbps > hi) return 0;
    if (simple_roiQp == 0) return 0;
    if (kbps <= lo) return simple_roiQp;

    // pct = 25 .. 100 linearly as kbps falls from 11000 -> 2000
    int x = hi - kbps; // 0 .. 8999
    int pct = 25 + (x * 75 + (span / 2)) / span; // rounded

    long v = (long)simple_roiQp * (long)pct;
    if (v >= 0) v = (v + 50) / 100;
    else        v = (v - 50) / 100;  // round negatives correctly with trunc-to-zero division
    return (int)v;
}


static void update_bitrate(int m, int width, float scale) {
    float total = (throughput_pct / 100.0f) * (scale > 0.0f ? scale : 1.0f);
    int kb = (width && pr[MCS_IDX(m)]) ? (int)(pr[MCS_IDX(m)] * (width / 20.0f) * total) : 0;
    int target = kb > 0 ? kb : 1500;
    if (target < 100) target = 100; else if (target > 52000) target = 52000;

    osd_kb = reduced_power_mode ? 100 : target;
    osd_colour_mod = (m < -2) ? 2 : ((m < 0) ? 5 : 3);

	int desired_qp = calc_roiQp_from_kbps(osd_kb);
    apply_roiQp(desired_qp);

    char cmd[256]; snprintf(cmd, sizeof(cmd), api_cmd, osd_kb);
    int __attribute__((unused)) ret = run_local_api_cmd(cmd);

    VPRINT(2, "bitrate: mcs=%d width=%d scale=%.2f total=%.2f kb=%d", m, width, scale, total, osd_kb);
}

// --- Step-based PUBQ adaptation ---
static inline float queue_fill_norm(int q, int nominal, bool driver_reports_used) {
    if (nominal <= 0) return 1.0f;
    float r = (driver_reports_used)
        ? (float)q / (float)nominal
        : 1.0f - ((float)q / (float)nominal);
    if (r < 0.f) r = 0.f;
    if (r > 1.f) r = 1.f;
    return r;
}

static void pubq_adapt_step(int q_raw, int nominal, bool reports_used,
                            int mcs, int width)
{
    uint64_t now = now_ms();

    float f = queue_fill_norm(q_raw, nominal, reports_used);
    pubq_f_short += 0.18f * (f - pubq_f_short);
    pubq_f_long  += 0.04f * (f - pubq_f_long);

    // --- Quadratic dip tracking for adaptive thresholds ---
    if (enable_thresh_autoadapt) {
        if (f < 0.9f) {
            float severity = 0.9f - f;
            dip_score += severity * severity;
        }

        if (now - dip_last_window_ms >= 2000) {
            if (dip_score > 0.1f) {
                dynamic_shift += thresh_adapt_gain * dip_score;
                if (dynamic_shift > max_thresh_shift) dynamic_shift = max_thresh_shift;
                if (dynamic_shift < -max_thresh_shift) dynamic_shift = -max_thresh_shift;
                VPRINT(1, "Dynamic shift updated: %.2f (dip_score=%.2f)", dynamic_shift, dip_score);
            } else {
                dynamic_shift *= 0.9f;
            }
            dip_score = 0.0f;
            dip_last_window_ms = now;
        }
    }
}

void write_external_osd(int used_rssi, int hw_mcs, int rssi_local, int rssi_udp,
                        int bw, int txpower, int channel, const char *gs_string) {
    FILE *fp = fopen(EXT_MSG_PATH, "w");
    if (fp) {
        int from_udp = (rssi_udp != -1);
        const char *used_src = from_udp ? mcs_source : "local";
        const char *label = from_udp ? mcs_source : "LQ";

        // Always write all OSD-capable fields here when external OSD is enabled,
        // regardless of which on-screen items are currently toggled.
        fprintf(fp,
            "used_rssi=%d\n"
            "used_source=%s\n"
            "label=%s\n"
            "mcs=%d\n"
            "target_kbps=%d\n"
            "width_mhz=%d\n"
            "channel=%d\n"
            "txpwr_set_mbm=%d\n"
            "txpwr_dbm=%d\n"
            "rssi_local=%d\n"
            "rssi_udp=%d\n"
            "osd_level=%d\n"
            "show_signal_bars=%d\n"
            "show_tx_power=%d\n"
            "q=%d\n"
            "dynamic_shift=%.2f\n"
            "cuts=%d\n"
            "antenna_mismatch=%d\n"
            "reduced_power_mode=%d\n"
            "snr_norm=%d\n"
            "iface=%s\n"
            "driver=%s\n"
            "active_ip=%s\n"
            "active_mac=%s\n"
            "gs_string=%s\n",
            used_rssi, used_src, label, hw_mcs,
            osd_kb, bw, channel, txpower,
            (txpower == INT_MIN ? -1 : (txpower + 50) / 100),
            rssi_local, rssi_udp,
            osd_level, show_signal_bars, show_tx_power,
            osd_q, dynamic_shift, osd_cuts,
            antenna_mismatch, reduced_power_mode,
            latest_snr_norm,
            iface, driver_name,
            active_ip[0] ? active_ip : "", active_mac[0] ? active_mac : "",
            gs_string ?: "");
        fclose(fp);
    }
}

void print_bar(FILE *f, const char *label, int val) {
    if (val < 0 || val > 100) return;

    const int bar_width = 15;
    int filled = val * bar_width / 100;

    fprintf(f, "[");
    for (int i = 0; i < bar_width; i++) {
        fputc(i < filled ? '#' : '.', f);
    }
    fprintf(f, "] [%d%%] %s\n", val, label);
}

void write_status(int int_rssi, int hw_mcs, int width, int from_udp,
                  int rssi_local, int rssi_udp, int txpower, int channel) {
    static int cleared;
    if (!osd_level) {
        if (!cleared) {
            cleared = 1;
            FILE *f = fopen(MSG_PATH, "w");
            if (f) { fputs("\n", f); fclose(f); }
        }
        return;
    }
    cleared = 0;

    FILE *f = fopen(MSG_PATH, "w");
    if (!f) return;

    char osd_mod[64];
    strncpy(osd_mod, osd_params, sizeof(osd_mod));
    osd_mod[63] = '\0';

    // --- Apply OSD color modification ---
    char *p = strstr(osd_mod, "&L");
    if (p && p[2]) p[2] = '0' + osd_colour_mod;

    // --- Apply OSD font scaling ---
    char *fpos = strstr(osd_mod, "&F");
    if (fpos && isdigit((unsigned char)fpos[2]) && isdigit((unsigned char)fpos[3])) {
        int orig = (fpos[2] - '0') * 10 + (fpos[3] - '0');
        int scaled = (int)(orig * osd_scale);
        if (scaled < 10) scaled = 10;
        if (scaled > 99) scaled = 99;
        snprintf(fpos, 5, "&F%02d", scaled);  // needs 5 bytes for "&Fxx\0"
    }

    const char *label = from_udp ? mcs_source : "LQ";
    char bitrate[32];
    snprintf(bitrate, sizeof(bitrate),
             reduced_power_mode ? "REDUCING_HEAT" : "%dkbps", osd_kb);

    if (osd_level == 1)
        fprintf(f, "%s%s:%d%%\n", osd_mod, label, int_rssi);
    else if (osd_level == 2)
        fprintf(f, "%s%s:%d%% | &B\n", osd_mod, label, int_rssi);
    else
        fprintf(f, "%s%s:%d%% | MCS:%d | %s | &B | CPU:&C,&Tc | TX:&Wc&G8\n",
                osd_mod, label, int_rssi, hw_mcs, bitrate);

    if (show_signal_bars) {
        if (rssi_local >= 0) print_bar(f, "uplink", rssi_local);
        if (rssi_udp >= 0)   print_bar(f, "downlink", rssi_udp);
    }

    if (show_tx_power) {
        char extra[64] = "";
        if (channel > 0 && width > 0)
            snprintf(extra, sizeof(extra), " | Ch:%d-%dmhz", channel, width);
        if (txpower == INT_MIN) fprintf(f, "pw:?%s", extra);
        else                    fprintf(f, "pw:%d%s", txpower, extra);

        if (osd_q >= 0) {
            fprintf(f, " | q:%d", osd_q);
            fprintf(f, ",shift:%.1f", dynamic_shift);
        }

        if (enable_fps_cut) {
            fprintf(f, " | cuts:%d", osd_cuts);
        }
    }

	if (antenna_mismatch && strcmp(driver_name, "rtl8733bu") != 0) {
		fprintf(f, "\nWarning! Check VTX antennas! Persistent mismatch detected\n");
	}

    fclose(f);
}

static void *mcs_worker(void *arg) {
    (void)arg;
    while (!mcs_stop) {
        pthread_mutex_lock(&mcs_lock);
        while (!mcs_job_pending && !mcs_stop)
            pthread_cond_wait(&mcs_cond, &mcs_lock);
        if (mcs_stop) { pthread_mutex_unlock(&mcs_lock); break; }
        mcs_job_t job = mcs_job;
        mcs_job_pending = 0;
        pthread_mutex_unlock(&mcs_lock);

        int nu = job.mcs, cur = job.cur_mcs, w = job.width;
        int hw_nu = nu < 0 ? 0 : nu, hw_cur = cur < 0 ? 0 : cur;

        if (hw_nu > hw_cur) {
            set_power(hw_nu);
            usleep(30000);
            do {
            int __ret = set_mcs_width(hw_nu, w);
            if (__ret == 0) atomic_store(&hw_mcs, hw_nu);
            usleep(20000);
        } while(0);

        }

        update_bitrate(nu, w, job.scale);

        if (hw_nu < hw_cur) {
			usleep(50000);
            do {
            int __ret = set_mcs_width(hw_nu, w);
            if (__ret == 0) atomic_store(&hw_mcs, hw_nu);
            usleep(20000);
        } while(0);

            set_power(hw_nu);
        }
    }
    return NULL;
}

static int maybe_adjust_mcs(int *mcs, int pct, int width, const int *thresh) {
    static struct timespec last_up = {0,0};
    int cap = (width == 20) ? max_mcs : (width == 40) ? max_mcs_40 : max_mcs_80;
    int cur = *mcs, nu = cur;

    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);

    long diff_ms = (now.tv_sec - last_up.tv_sec) * 1000 +
                   (now.tv_nsec - last_up.tv_nsec) / 1000000;
    int up_allowed = (diff_ms >= up_cooldown_ms);

    // Upward confidence gate (anti-flap): require pct to remain above next-step threshold
    // for UPWARD_CONFIDENCE_LOOPS consecutive evaluations before allowing any step-up.
    static int up_conf = 0;
    static int up_conf_mcs = -999;
    static int up_conf_width = 0;

    if (cur != up_conf_mcs || width != up_conf_width) {
        up_conf = 0;
        up_conf_mcs = cur;
        up_conf_width = width;
    }

    int up_ok = 0;
    if (cur < cap) {
        int req = (int)(thresh[MCS_IDX(cur + 1)] * (1.0f + H_pct));
        if (pct >= req) {
            if (up_conf < upward_confidence_loops) up_conf++;
        } else {
            up_conf = 0;
        }
        up_ok = (up_conf >= upward_confidence_loops);
    }

    /* Step down aggressively if under threshold (can skip levels) */
    while (nu > MCS_MIN && pct < thresh[MCS_IDX(nu)]) {
        nu--;
    }

    /* Step up aggressively if comfortably above thresholds (can skip levels) */
    if (nu == cur && cur < cap && up_allowed && up_ok) {
        while (nu < cap &&
               pct >= (int)(thresh[MCS_IDX(nu + 1)] * (1.0f + H_pct))) {
            nu++;
        }
        if (nu > cur) {
            last_up = now;
            up_allowed = 0; // reset cooldown
            up_conf = 0;
        }
    }

    if (nu == cur)
        return 0;

    pthread_mutex_lock(&mcs_lock);
    mcs_job = (mcs_job_t){
        .mcs      = nu,
        .width    = width,
        .scale    = 1.0f,
        .cur_mcs  = *mcs
    };
    mcs_job_pending = 1;
    pthread_cond_signal(&mcs_cond);
    pthread_mutex_unlock(&mcs_lock);

    *mcs = nu;
    return 1;
}

int main(int argc, char *argv[]) {
    int opt;

    while ((opt = getopt(argc, argv, "v")) != -1) {
        if (opt == 'v') {
            verbose++;
        }
    }

    atexit(cleanup);
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
	signal(SIGHUP, handle_signal);

    VPRINT(1, "Starting with verbosity level %d", verbose);

    load_config();
    VPRINT(1, "Config loaded: iface=%s, udp_port=%d", iface, udp_port);

    if (find_proc_path(RATE_SUBPATH_FMT, rate_path, sizeof(rate_path), iface) < 0) {
        fprintf(stderr, "Rate ctl not found\n");
        return 1;
    }
    VPRINT(1, "Found rate_ctl path: %s", rate_path);

    // After rate path detection, driver_name is known. Probe cheap hooks if requested.
    detect_cheap_hooks();

    if (find_proc_path(TRX_SUBPATH_FMT, proc_trx_path, sizeof(proc_trx_path), iface) < 0) {
        if (!(use_cheap_proc && (!strcmp(driver_name, "rtl88x2eu") || !strcmp(driver_name, "rtl88x2cu")) && cheap_rssi_avail)) {
            fprintf(stderr, "RSSI debug not found\n");
            return 1;
        } else {
            VPRINT(1, "RSSI debug not found; using cheap RSSI hooks");
            proc_trx_path[0] = '\0';
        }
    } else {
	    VPRINT(1, "Found trx_info_debug path: %s", proc_trx_path);
    }
	
    if (find_proc_path(AP_INFO_SUBPATH_FMT, proc_ap_info_path, sizeof(proc_ap_info_path), iface) < 0) {
        fprintf(stderr, "ap_info not (yet) found\n");
    }
	VPRINT(1, "Found proc_ap_info_path: %s", proc_ap_info_path);

	if (find_proc_path(TRX_PUBQ_FMT, proc_pubq_path, sizeof(proc_pubq_path), iface) < 0) {
		VPRINT(1, "PUBQ info path not found");
		proc_pubq_path[0] = '\0';
	} else {
		VPRINT(1, "Found proc_pubq_path: %s", proc_pubq_path);
	}
    summarize_sources();


    VPRINT(1, "Detected driver: %s", driver_name);

    apply_config();
    pthread_create(&mcs_thread, NULL, mcs_worker, NULL);
	pthread_create(&rssi_thread, NULL, rssi_worker, NULL);
    start_ping();

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    int yes = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
	#ifdef SO_REUSEPORT
		setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
	#endif

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_ANY),
        .sin_port = htons(udp_port)
    };
    if (bind(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }

    int mcs = 0;
	int width = 0;
	int channel = 0;
	get_ap_info(&width, &channel);
    int loop_idle = 0, print_counter = 0, recv_counter = 0;
    int rssi_local = -1, rssi_udp = -1;
	static int low_rssi_mode;
	char gs_string[1024];
	int maj_fps = 60;
	int cfg_fps = maj_fps;             // FPS from last known Majestic config (restore target)
	int thermal_fps_applied = -1;      // last FPS applied via mi_sensor
	static float last_scale = 1.0f;
	static uint64_t last_cut_ms = 0;
	static float pubq_trend_penalty = 0.0f;

	{
		int _rfps = reapply_fps_from_majestic_yaml("startup/apply_config");
		if (_rfps > 0) { cfg_fps = _rfps; maj_fps = _rfps; thermal_fps_applied = _rfps; }
	}

    set_mcs_width(mcs, width);
    atomic_store(&hw_mcs, mcs);
    update_bitrate(mcs, width, 1.0f);
    printf("Listening on UDP/%d, iface=%s\n", udp_port, iface);

    while (1) {
        fd_set rfds;
        struct timeval tv = {0, 100000};
        FD_ZERO(&rfds);
        FD_SET(sock, &rfds);

        int activity = select(sock + 1, &rfds, NULL, NULL, &tv);
		
		if (active_mac[0] == 0) update_active_mac_from_ip();

		// --- Apply predictive RSSI logic ---
		int last, fast, slow;
		pthread_mutex_lock(&rssi_mutex);
		last = rssi_last();
		fast = latest_rssi_fast;
		slow = latest_rssi_slow;
		pthread_mutex_unlock(&rssi_mutex);

		if (slow > 0) {
			rssi_local = slow;  // base on slow EMA

			if (fast > 0 && fast < slow) {
				int gap = slow - fast;

				// Predictive pessimism: bias below fast by half the gap
				//rssi_local = fast - gap / 2;
				rssi_local = fast - gap * predict_multi;
				
				// Clamp
				if (rssi_local < 0) rssi_local = 0;
			}
		} else {
			rssi_local = last;  // fallback if EMA not ready
		}

        int used_rssi = -1;
        gs_string[0] = '\0';
		
		// --- Build adaptive thresholds ---
		int shifted_thresh[MCS_COUNT];
		memcpy(shifted_thresh, pct_thresh, sizeof(shifted_thresh));

		if (enable_thresh_autoadapt && fabsf(dynamic_shift) > 0.01f) {
			for (int i = 0; i < MCS_COUNT; i++) {
				float frac = i / (float)(MCS_COUNT - 1);
				int s = shifted_thresh[i] + (int)(dynamic_shift * frac);

				if (s < 1) s = 1;
				if (s > 100) s = 100;
				shifted_thresh[i] = s;
			}

			// enforce monotonic increase
			for (int i = 1; i < MCS_COUNT; i++) {
				if (shifted_thresh[i] <= shifted_thresh[i - 1]) {
					shifted_thresh[i] = shifted_thresh[i - 1] + 1;
					if (shifted_thresh[i] > 100) shifted_thresh[i] = 100;
				}
			}
		}

		if (activity > 0 && FD_ISSET(sock, &rfds)) {
			char buf[1024];
			socklen_t plen = 0;
			int n = recvfrom(sock, buf, sizeof(buf) - 1, 0, NULL, &plen);
			if (n > 0) {
				buf[n] = '\0';

				char *comma = strchr(buf, ',');
				if (comma) {
					*comma = '\0';
					strncpy(gs_string, comma + 1, sizeof(gs_string) - 1);
					gs_string[sizeof(gs_string) - 1] = '\0';
					gs_string[strcspn(gs_string, "\r\n")] = '\0';
				}

				int val = atoi(buf);
				if (val >= 0 && val <= 100) {
					// Apply log scaling
					rssi_udp = apply_log_scale(val);

					used_rssi =
						!strcmp(mcs_source,"lowest")  ? ((rssi_local>=0&&rssi_udp>=0)?(rssi_local<rssi_udp?rssi_local:rssi_udp):(rssi_udp>=0?rssi_udp:rssi_local)) :
						!strcmp(mcs_source,"highest") ? ((rssi_local>=0&&rssi_udp>=0)?(rssi_local>rssi_udp?rssi_local:rssi_udp):(rssi_udp>=0?rssi_udp:rssi_local)) :
						!strcmp(mcs_source,"both")    ? ((rssi_local>=0&&rssi_udp>=0)?(rssi_local+rssi_udp)/2:-1) :
						!strcmp(mcs_source,"downlink")? rssi_udp :
						!strcmp(mcs_source,"uplink")  ? rssi_local :
														rssi_udp;
					loop_idle = 0;
				}

			}
		} else if (++loop_idle >= udp_timeout_loops) {
			rssi_udp = -1;
			if (rssi_local >= 0)
				used_rssi = rssi_local;
		}

		int pubq = -1;
		if (cheap_pubq_avail) {
			int v;
			if (read_int_file(proc_pubq_free_path, &v) == 0) {
				pubq = v;
				osd_q = pubq;
			}
		} else if (proc_pubq_path[0]) {
			pubq = read_pubq(proc_pubq_path);
			osd_q = pubq;
		}

		if (pubq >= 0) {
			pubq_adapt_step(pubq, PUBQ_NOMINAL, cheap_pubq_avail ? cheap_pubq_reports_used : true, mcs, width);
		}

		/* --- Apply and update OSD --- */
		
		if (used_rssi >= 0) {
			maybe_adjust_mcs(&mcs, used_rssi, width, shifted_thresh);

			int osd_mcs_now = atomic_load(&hw_mcs);
			static int last_osd_mcs = INT_MIN;
			int do_status = (++recv_counter % status_cnt == 0);
			if (osd_mcs_now != last_osd_mcs) { last_osd_mcs = osd_mcs_now; do_status = 1; }

			if (do_status) {
				int txpower = atomic_load(&hw_txpower_set_mbm);
				write_status(used_rssi, osd_mcs_now, width, (rssi_udp >= 0),
							 rssi_local, rssi_udp, txpower, channel);

				if (external_osd) {
					write_external_osd(used_rssi, osd_mcs_now, rssi_local, rssi_udp,
									   width, txpower, channel, gs_string);
				}
			}
		}
		
		if (++print_counter % refresh_cnt == 0) {
			int nw = 0, nc = 0;
			get_ap_info(&nw, &nc);
			if (nw && nw != width) {
				VPRINT(1, "Width %d→%d, reset MCS", width, nw);
				width = nw; mcs = 0; set_mcs_width(mcs, width); update_bitrate(mcs, width, 1.0f);
			}
			if (nc && nc != channel) {
				VPRINT(1, "Channel %d→%d", channel, nc);
				channel = nc;
			}
		}

		
		if (enable_fps_cut && print_counter % 31 == 0) {
			{ int _fps=0; if (majestic_get_video0_fps(&_fps) == 0 && _fps > 0) maj_fps = _fps; }
		}
		
        /* --- Thermal check --- */
		if (print_counter % 51 == 0) {
			int vtx=0, atmp=0, max, prev=reduced_power_mode;
			char buf[256], path[128]; FILE *f;

			// vtx temp: cache fd
			{ static int fd=-2;
			  if (fd==-2) fd=open("/sys/devices/system/cpu/cpufreq/temp_out", O_RDONLY|O_CLOEXEC);
			  if (fd>=0) {
				int n;
				n=(int)pread(fd,buf,sizeof(buf)-1,0);
				if (n>0) { buf[n]=0; char *p=buf; while (*p && ((*p<'0'||*p>'9') && *p!='-')) p++; if (*p) vtx=atoi(p); }
			  }
			}
			if (!vtx && (f=popen("ipcinfo --temp","r"))) { if (fgets(buf,sizeof(buf),f)) vtx=atoi(buf); pclose(f); }

			snprintf(path,sizeof(path),"/proc/net/%.64s/wlan0/thermal_state",driver_name);
			if (proc_read_cached(path, buf, sizeof(buf)) > 0) {
			  char *p=buf;
			  while ((p=strstr(p,"temperature"))!=NULL) {
				char *q=p;
				while (*q && *q!=':') q++;
				while (*q && ((*q<'0'||*q>'9') && *q!='-')) q++;
				if (*q) {
				  int t=atoi(q);
				  if (!strcmp(driver_name,"rtl88x2eu") || !strcmp(driver_name,"rtl88x2cu")) { if (t>atmp) atmp=t; p=q+1; continue; }
				  atmp=t; break;
				}
				p=p+11;
			  }
			}

			max = (vtx>atmp)?vtx:atmp;
			/* Exit criteria (no ramp): require max <= (HIGH_TEMP - 5°C) continuously for 5 seconds.
	   This prevents immediate bounce-back when restoring high FPS. */
			if (!prev) {
				reduce_heat_cool_since_ms = 0;
				reduced_power_mode = (max >= high_temp);
			} else {
				if (max <= (high_temp - 5)) {
					uint64_t tnow = now_ms();
					if (reduce_heat_cool_since_ms == 0) reduce_heat_cool_since_ms = tnow;
					if ((tnow - reduce_heat_cool_since_ms) >= 5000ULL) {
						reduced_power_mode = 0;
						reduce_heat_cool_since_ms = 0;
					} else {
						reduced_power_mode = 1;
					}
				} else {
					reduce_heat_cool_since_ms = 0;
					reduced_power_mode = 1;
				}
			}
			if (reduced_power_mode != prev) {
	VPRINT(1,"Thermal state changed → reduced_power_mode=%d (temp=%d)",reduced_power_mode,max);
	update_bitrate(mcs,width,1.0f);

	// Enter: force 30fps immediately. Exit: restore to current configured/original FPS.
	if (reduced_power_mode) {
		// Capture current configured fps at the moment we enter thermal mode (in case user changed it live)
		{ int live_fps = 0;
		  if (majestic_get_video0_fps(&live_fps) == 0 && live_fps > 0) { cfg_fps = live_fps; maj_fps = live_fps; }
		}

		int desired = 30;
		if (desired != thermal_fps_applied) {
			if (mi_sensor_set_fps(desired) == 0) {
				thermal_fps_applied = desired;
				VPRINT(1, "Thermal FPS → %d (enter REDUCING_HEAT)", thermal_fps_applied);
			}
		}
	} else {
		// Restore FPS from Majestic config file (majestic.yaml) on exit.
		{ int file_fps = 0;
		  if (majestic_get_video0_fps_from_yaml(&file_fps) == 0 && file_fps > 0) { cfg_fps = file_fps; maj_fps = file_fps; }
		}

		int restore = (cfg_fps > 0) ? cfg_fps : maj_fps;
		if (restore <= 0) restore = 60;

		if (restore != thermal_fps_applied) {
			if (mi_sensor_set_fps(restore) == 0) {
				thermal_fps_applied = restore;
				VPRINT(1, "Thermal FPS → %d (restore)", thermal_fps_applied);
			}
		}
	}
} else if (reduced_power_mode) {
	// While hot: once 5°C over threshold drop to 20fps, then 10fps, then 5fps.
	int delta = max - high_temp;
	int desired = 30;
	if (delta >= 7) desired = 5;
	else if (delta >= 6) desired = 10;
	else if (delta >= 5) desired = 20;

	if (desired != thermal_fps_applied) {
		if (mi_sensor_set_fps(desired) == 0) {
			thermal_fps_applied = desired;
			VPRINT(1, "Thermal FPS → %d (temp=%d, thres=%d)", thermal_fps_applied, max, high_temp);
		}
	}
}
		}
	
		if (reload_requested) {
			VPRINT(1, "Reloading configuration...");
			load_config();
			apply_config();
			{
				int _rfps = reapply_fps_from_majestic_yaml("reload/apply_config");
				if (_rfps > 0) { cfg_fps = _rfps; maj_fps = _rfps; thermal_fps_applied = _rfps; }
			}
			proc_cache_reset();
			detect_cheap_hooks();
			summarize_sources();
			start_ping();
			maybe_adjust_mcs(&mcs, 10, width, shifted_thresh); // simulate 10% rssi
			//update_bitrate(0, width, 1.0f);
			//set_mcs(0);
			//set_power(0);
			reload_requested = 0;
			VPRINT(1, "Configuration reloaded.");
		}

    }
	
    close(sock);
    mcs_stop = 1;
    pthread_cond_signal(&mcs_cond);
    pthread_join(mcs_thread, NULL);

    return 0;
}
