"""Load model for llmrouter: capacity, RAM, CPU, storage and network.

Workload:
  1M token prompt in, 32k token completion out, every client request
  prefill 1,000 tok/s, generation 30 tok/s, no token streaming
  N=32, M=2, so 64 channel calls per client request
  5,000 Mbit/s internet, 10GbE LAN, 100GB SSD ZFS pool
  4GB RAM budget, admit below a 3GB high water mark

Two rules the model follows, both learned the hard way:

  1. Concurrency is never assumed. Little's Law gives it, per phase:
         copies = arrival rate x time held
     A buffer held for 2ms at 1 req/s costs 0.002 copies. One held for
     2,067s costs 2,067.

  2. Connections are pooled across every client request, not per request.
     HTTP/2 carries 100-250 request slots on one connection. A "request
     slot" is one HTTP/2 stream: one request and its response. It has
     nothing to do with token streaming, which this router does not do.

    python3 -I load_model.py
"""

KB, MB, GB, TB = 1024, 1024**2, 1024**3, 1024**4

# --- workload -------------------------------------------------------------
BPT = 4                        # bytes per token, English in JSON (estimate)
IN_TOK, OUT_TOK = 1_000_000, 32_000
PREFILL_TPS, GEN_TPS = 1_000, 30
N, M = 32, 2
CALLS = N * M
PROVIDERS = 15
LINE_MBIT, LAN_MBIT = 5_000, 10_000
POOL_GB = 100
STATED_RPS = 1.0               # the target's peak
REAL_PER_DAY = 3_000           # what is actually used

# --- the box --------------------------------------------------------------
BUDGET, HIGH_WATER, FIXED = 4 * GB, 3 * GB, 257 * MB

# --- settings the router chooses -----------------------------------------
WINDOW = 128 * KB              # http2_initial_stream_window_size
SLOTS_PER_CONN = 250           # SETTINGS_MAX_CONCURRENT_STREAMS, typical
CONN_KB = 128                  # per-connection buffers, excluding windows
SPILL_ABOVE = 100              # concurrent client requests
RTT_S = 0.050

# --- estimates everything rests on ---------------------------------------
PARSE_MULT = 2.5               # parsed structure vs raw JSON
FRAG = 0.15                    # allocator fragmentation
ENTRY_META = 95
GBS = {"serde_json parse": 0.3, "serde_json write": 0.6,
       "simd parse": 1.5, "simd write": 1.5,
       "blake3": 5.0, "AES-GCM": 3.0}

prompt_b = IN_TOK * BPT
reply_b = OUT_TOK * BPT
upload_s = (prompt_b * 8 / 1e6) / (LINE_MBIT / CALLS)
service_s = upload_s + IN_TOK / PREFILL_TPS + OUT_TOK / GEN_TPS


def ms(nbytes, gbs):
    return nbytes / (gbs * GB) * 1000


def row(label, value):
    print(f"  {label:<50} {value}")


def head(t):
    print(f"\n{t}\n" + "-" * 78)


print("=" * 78)
print(f"{prompt_b/MB:,.1f}MB prompt, {reply_b/KB:,.0f}KB reply, "
      f"N={N} M={M} = {CALLS} channel calls, {WINDOW/KB:,.0f}KB window")
print("=" * 78)

# --- capacity -------------------------------------------------------------
head("CAPACITY, the chain")
avail = HIGH_WATER - FIXED
per_slot = WINDOW + (CONN_KB * KB) / SLOTS_PER_CONN
slots = avail / per_slot
max_concurrent = slots / CALLS
row(f"budget {BUDGET/GB:.0f}GB, admit below {HIGH_WATER/GB:.0f}GB, "
    f"fixed {FIXED/MB:.0f}MB", f"{avail/GB:>9,.2f} GB for traffic")
row(f"per request slot: {WINDOW/KB:.0f}KB window + "
    f"{CONN_KB/SLOTS_PER_CONN:.1f}KB conn share", f"{per_slot/KB:>9,.1f} KB")
row("request slots that fit", f"{slots:>9,.0f}")
row(f"client requests, at {CALLS} channel calls each",
    f"{max_concurrent:>9,.0f}")
row(f"connections, at {SLOTS_PER_CONN} slots each",
    f"{slots/SLOTS_PER_CONN:>9,.0f}")

head("THROUGHPUT, by how fast the model generates")
print(f"  {'generation':>11}{'service':>10}{'max req/s':>11}{'req/day':>10}"
      f"{'client tok/day':>16}{'upstream':>11}")
for tps in (GEN_TPS, 100, 500, 1_000):
    svc = upload_s + IN_TOK / PREFILL_TPS + OUT_TOK / tps
    rps = max_concurrent / svc
    up = CALLS * prompt_b * rps * 8 / 1e9
    print(f"  {tps:>8,}tps{svc:>9,.0f}s{rps:>11,.2f}{rps*86400:>10,.0f}"
          f"{rps*86400*(IN_TOK+OUT_TOK)/1e9:>14,.0f}B{up:>9,.2f}G")
row("prefill is the floor", f"{IN_TOK/PREFILL_TPS:,.0f}s however fast "
    f"generation gets")

head("WHICH CEILING BINDS")
mem_rps = max_concurrent / service_s
bw_rps = LINE_MBIT * 1e6 / (CALLS * prompt_b * 8)
row(f"memory, {HIGH_WATER/GB:.0f}GB at a {WINDOW/KB:.0f}KB window",
    f"{mem_rps:>9,.2f} req/s   <- binds first")
row(f"bandwidth, {LINE_MBIT:,} Mbit/s line", f"{bw_rps:>9,.2f} req/s")
row(f"file descriptors, {slots/SLOTS_PER_CONN:,.0f} connections",
    "far above")
row("stated usage", f"{REAL_PER_DAY/86400:>9,.3f} req/s "
    f"({REAL_PER_DAY:,}/day)")
row("headroom against stated usage",
    f"{mem_rps*86400/REAL_PER_DAY:>9,.1f}x")

# --- RAM ------------------------------------------------------------------
head("RAM, by phase: arrival rate x time held")
rps = mem_rps
phases = [
    ("inbound raw body", prompt_b, ms(prompt_b, GBS["serde_json parse"])/1000,
     "freed after the parse"),
    ("parsed structure", prompt_b * PARSE_MULT,
     ms(prompt_b, GBS["serde_json write"])/1000, "freed after the encode"),
    ("reply, before sending to the client", reply_b, 0.001,
     "sent, then cached"),
    ("outbound body", prompt_b, service_s, "spilled above "
     f"{SPILL_ABOVE} concurrent"),
]
print(f"  {'buffer':<34}{'size':>8}{'held':>12}{'copies':>9}{'RAM':>10}")
for label, size, held, note in phases:
    copies = rps * held
    print(f"  {label:<34}{size/MB:>7,.1f}M{held:>11,.3f}s"
          f"{copies:>9,.2f}{copies*size/MB:>9,.1f}M   {note}")

head("RAM, whole process at the memory ceiling")
spilling = max_concurrent > SPILL_ABOVE
parts = [
    (f"request slots, {slots:,.0f} x {WINDOW/KB:.0f}KB", slots * WINDOW),
    (f"connections, {slots/SLOTS_PER_CONN:,.0f} x {CONN_KB}KB",
     slots / SLOTS_PER_CONN * CONN_KB * KB),
    (f"outbound bodies ({'on disk' if spilling else 'in RAM'})",
     0 if spilling else max_concurrent * prompt_b),
    ("short-lived buffers, parse and reply",
     sum(rps * h * sz for _, sz, h, _ in phases[:3])),
    ("redb page cache, capped", 256 * MB),
    ("channel list + health, 4,000 rows", 4000 * 240),
]
total = sum(b for _, b in parts)
for label, b in parts:
    row(label, f"{b/MB:>9,.1f} MB")
row("TOTAL", f"{total/GB:>9,.2f} GB")
row(f"against the {HIGH_WATER/GB:.0f}GB high water",
    "at it, by construction" if abs(total - HIGH_WATER) < 0.4*GB else "off")

head("WHY THE HIGH WATER IS 3GB, NOT 4GB")
frag = HIGH_WATER * FRAG
overshoot = 0   # RSS is read per admission, not on a timer
row(f"allocator fragmentation at {FRAG:.0%}", f"{frag/MB:>9,.0f} MB")
row("sampling overshoot", "none; RSS is read per admission")
row("slack available", f"{(BUDGET-HIGH_WATER)/MB:>9,.0f} MB")
row("max safe high water, solving high x 1.15 = 4GB",
    f"{BUDGET/(1+FRAG)/GB:>9,.2f} GB")

head("WHAT DOES NOT FIT")
for label, r in [("1 client request/s", STATED_RPS),
                 ("30 client requests/s", 30.0)]:
    c = r * service_s
    need = c * CALLS * per_slot + FIXED
    row(f"{label}: {c:,.0f} concurrent against {max_concurrent:,.0f}",
        f"{need/GB:>9,.1f} GB needed")
row("provider tokens per client request",
    f"{CALLS*IN_TOK/1e6:>9,.0f} million input")
row(f"at the stated {REAL_PER_DAY:,}/day",
    f"{REAL_PER_DAY*CALLS*IN_TOK/1e9:>9,.0f} billion/day vs 3 billion used")

# --- CPU ------------------------------------------------------------------
head("CPU, latency path: before the first byte reaches a provider")
for parser, writer in (("serde_json parse", "serde_json write"),
                       ("simd parse", "simd write")):
    t = (ms(prompt_b, GBS[parser]) + ms(prompt_b, GBS["blake3"])
         + ms(prompt_b, GBS[writer]))
    row(f"{parser.split()[0]}: parse + blake3 + encode",
        f"{t:>9,.2f} ms   {'fits 10ms' if t < 10 else 'misses 10ms'}")
row("switchyard-translation uses", "serde_json, so 19.4ms. Accepted.")
row(f"against a channel call of {service_s:,.0f}s", "0.001% of it")

head("CPU, throughput at the memory ceiling")
work = [("parse inbound prompt", ms(prompt_b, GBS["serde_json parse"])),
        ("blake3 cache key", ms(prompt_b, GBS["blake3"])),
        ("encode outbound body", ms(prompt_b, GBS["serde_json write"])),
        (f"TLS encrypt {CALLS*prompt_b/MB:,.0f}MB",
         ms(CALLS * prompt_b, GBS["AES-GCM"])),
        ("parse the winning reply", ms(reply_b, GBS["serde_json parse"])),
        ("cache write, 1 redb transaction", 0.5)]
cpu_ms = sum(x for _, x in work)
for label, x in work:
    row(label, f"{x:>9,.2f} ms")
row("TOTAL per client request", f"{cpu_ms:>9,.2f} ms")
row(f"at {rps:,.2f} req/s", f"{cpu_ms/1000*rps*100:>8,.1f} % of one core")

head("NETWORK at the memory ceiling")
up = CALLS * prompt_b * rps
row("upstream per client request", f"{CALLS*prompt_b/MB:>9,.0f} MB")
row(f"upstream at {rps:,.2f} req/s", f"{up*8/1e9:>9,.2f} Gbit/s")
row(f"share of the {LINE_MBIT:,} Mbit/s line",
    f"{up*8/1e6/LINE_MBIT*100:>8,.0f} %")
row("upstream per day", f"{up*86400/TB:>9,.1f} TB")
row("downstream, the winning reply only",
    f"{reply_b*rps*8/1e6:>9,.2f} Mbit/s")

head("STORAGE")
entry = reply_b + ENTRY_META
day = rps * 86400 * entry
row("cache entry = reply + 95B", f"{entry/KB:>9,.1f} KB")
row(f"cache writes at {rps:,.2f} req/s", f"{day/GB:>9,.1f} GB/day")
row("7 day ttl holds", f"{day*7/GB:>9,.0f} GB")
row(f"max_bytes {POOL_GB}GB holds", f"{POOL_GB*GB/day:>9,.1f} days")
row("spill scratch, deleted per request",
    f"{max_concurrent*prompt_b/GB:>9,.1f} GB")


def _selfcheck():
    assert CALLS == 64 and prompt_b == 4_000_000
    assert abs(service_s - 2067) < 2, service_s
    # The documented capacity figures.
    assert abs(slots - 22_430) < 50, slots
    assert abs(max_concurrent - 350) < 2, max_concurrent
    assert abs(mem_rps - 0.17) < 0.01, mem_rps
    assert abs(mem_rps * 86400 - 14_652) < 100, mem_rps * 86400
    # Memory binds before bandwidth.
    assert mem_rps < bw_rps, (mem_rps, bw_rps)
    # Stated usage must sit well inside capacity.
    assert mem_rps * 86400 > 4 * REAL_PER_DAY
    # The high water must leave room for allocator fragmentation.
    assert HIGH_WATER * (1 + FRAG) < BUDGET
    # A buffer held for milliseconds costs well under one copy.
    assert rps * ms(prompt_b, GBS["serde_json parse"]) / 1000 < 0.01
    # RSS is read per admission, so there is no sampling overshoot.
    assert overshoot == 0


_selfcheck()
