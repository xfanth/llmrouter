"""Row checks for the 'query the channel list each time a channel is needed' scan.

The list is a vector sorted by intelligence, highest first. A query scans from
the top and returns rows that pass the predicate and are not in `excluded`.

One client request issues:
  - one opening query with LIMIT N, which fills the race window;
  - one query with LIMIT 1 for every channel call that fails, to replace it.

Tried rows are exactly the highest-ranked eligible rows, so they sit at the top
of every later scan. Query number j steps over the rows already tried plus the
rejected rows interleaved among them.

    python3 -I scan_cost.py
"""
import random

TOTAL = 4000      # channels in the list
N = 32            # race_width
CHEAP_NS = 2      # a row that fails an integer compare: no set lookup
HASH_NS = 15      # an eligible row: also costs an `excluded` lookup


def walk(total, eligible_frac, tried, n=N, seed=1):
    """Count row checks exactly, by walking a concrete list.

    Returns (row_checks, cheap_checks, hashed_checks, channels_tried, queries).
    """
    rng = random.Random(seed)
    rows = [rng.random() < eligible_frac for _ in range(total)]
    excluded = set()
    cheap = hashed = queries = 0

    def query(limit):
        """One scan from the top. Returns how many channels it found."""
        nonlocal cheap, hashed, queries
        queries += 1
        found = 0
        for i, ok in enumerate(rows):
            if not ok:
                cheap += 1
                continue
            hashed += 1
            if i not in excluded:
                excluded.add(i)
                found += 1
                if found == limit:
                    break
        return found

    got = query(n)                       # opening query, LIMIT N
    while got and len(excluded) < tried:  # replacements, LIMIT 1
        got = query(1)
    return cheap + hashed, cheap, hashed, len(excluded), queries


CASES = [
    ("1 Everything working", 1.00, N),
    ("2 Working, 200k prompt", 0.50, N),
    ("3 Best channels rate limited", 0.50, 500),
    ("4 Every key dead, big prompt", 0.50, TOTAL),
    ("5 Every key dead, cold start", 1.00, TOTAL),
]

def snapshot(total, eligible_frac):
    """Option A: one pass over the whole list at the start, no excluded set.

    Every row costs a cheap predicate check. The rows that pass are copied into
    a local vector, 8 bytes each if it holds references.
    """
    kept = int(total * eligible_frac)
    ns = total * CHEAP_NS + kept * COPY_NS
    return total, 1, ns


COPY_NS = 1       # pushing one reference into the local vector
REF_BYTES = 8     # one &Channel in option A's local vector
SET_BYTES = 16    # one u64 id in a HashSet, including table overhead
ROW_BYTES = 200   # one Channel row in the shared list
CONCURRENT = 30   # target concurrent client requests

def kb(n):
    return f"{n/1024:,.1f}KB"


hdr = (f"{'case':<30} {'tried':>6} | {'A rows':>9} {'A q':>5} {'A time':>9} {'A RAM':>9}"
       f" | {'B rows':>10} {'B q':>6} {'B time':>9} {'B RAM':>9}")
print(hdr)
print("-" * len(hdr))
for label, frac, tried in CASES:
    checks, cheap, hashed, got, queries = walk(TOTAL, frac, tried)
    b_ns = cheap * CHEAP_NS + hashed * HASH_NS
    a_rows, a_q, a_ns = snapshot(TOTAL, frac)
    # Option A holds a reference to every channel that passed the filter.
    a_ram = int(TOTAL * frac) * REF_BYTES
    # Option B holds one id per channel it has tried.
    b_ram = got * SET_BYTES
    print(f"{label:<30} {got:>6,} | {a_rows:>9,} {a_q:>5,} {a_ns/1e6:>8.3f}ms {kb(a_ram):>9}"
          f" | {checks:>10,} {queries:>6,} {b_ns/1e6:>8.3f}ms {kb(b_ram):>9}")

print()
print(f"shared channel list, both options: "
      f"{TOTAL:,} rows x {ROW_BYTES}B = {kb(TOTAL*ROW_BYTES)}, allocated once")
print(f"per-request RAM at {CONCURRENT} concurrent client requests:")
for label, frac, tried in CASES:
    _, _, _, got, _ = walk(TOTAL, frac, tried)
    a = int(TOTAL * frac) * REF_BYTES * CONCURRENT
    b = got * SET_BYTES * CONCURRENT
    print(f"  {label:<30} A {kb(a):>10}   B {kb(b):>10}")

print()
print("wall clock of the doomed walk itself, N=32 channel calls in flight:")
for label, per_call_ms in [("fast 401", 200), ("timeout", 60_000)]:
    rounds = TOTAL / N
    print(f"  {label:<9} {rounds:.0f} rounds x {per_call_ms}ms"
          f" = {rounds * per_call_ms / 1000:,.0f}s")


def _selfcheck():
    # Opening query alone fills the window: one query, N channels, N rows read.
    checks, _, _, got, queries = walk(100, 1.0, N)
    assert (queries, got, checks) == (1, N, N), (queries, got, checks)
    # Walking the whole list costs k(k+1)/2 minus the rows the opening query
    # read in one pass instead of N separate ones.
    checks, _, _, got, _ = walk(100, 1.0, 100)
    assert got == 100
    assert checks == 100 * 101 // 2 - (N - 1) * N // 2, checks
    # Rejected rows cap how many channels can ever be tried.
    _, _, _, got, _ = walk(100, 0.5, 100)
    assert got < 100, got


_selfcheck()
