#!/usr/bin/env python3
"""Rebuilds smaller (N, M) configurations from a run log's channel_copy lines.

One `channel_copy` line exists per hedge copy, on the walk's clock. This
script groups them by request, and replays a target (N, M):
  - trials fire in start_ms order, N at a time,
  - a trial answers at start_ms + ttft_ms of its first copy (index < M)
    that produced content, or fails at start_ms + its copies' max ms,
  - the first success ends the request; the rest are cancelled.
Estimate, not measurement: each trial keeps the duration it actually had.
"""
import argparse
import re
import statistics

COPY = re.compile(
    r"channel_copy, channel: (\S+), provider: (\S+)(?:, model: [^,]+)?, "
    r"copy_index: (\d+), start_ms: (\d+), ms: (\d+)(?:, ttft_ms: (\S+))?, outcome: (\S+)"
)
SPAN = re.compile(r"request with .*request_id: (\d+)")


def parse_log(path):
    runs = {}
    pending = None
    for line in open(path, errors="replace"):
        if "channel_copy" in line:
            m = COPY.search(line)
            if not m:
                continue
            pending = {
                "channel": m.group(1),
                "copy_index": int(m.group(3)),
                "start_ms": int(m.group(4)),
                "ms": int(m.group(5)),
                "ttft_ms": None if not m.group(6) else int(m.group(6)),
                "outcome": m.group(7).strip('"'),
            }
            continue
        m = SPAN.search(line)
        if m and pending is not None:
            rid = int(m.group(1))
            runs.setdefault(rid, []).append(pending)
            pending = None
    return runs


def replay(runs, n, m_max):
    lat = []
    for copies in runs.values():
        trials = {}
        for c in copies:
            if c["copy_index"] < m_max:
                trials.setdefault(c["channel"], []).append(c)
        ordered = [
            (
                min(c["start_ms"] for c in cs),
                next(
                    (
                        min(c["start_ms"] for c in cs)
                        + c["ttft_ms"]
                        for c in cs
                        if c["outcome"] == "success" and c["ttft_ms"]
                    ),
                    None,
                ),
                max(c["ms"] for c in cs),
            )
            for channel, cs in trials.items()
        ]
        ordered.sort(key=lambda t: t[0])
        slots = [0] * n
        for start, answer, fail in ordered:
            fire = slots.index(min(slots))
            if answer is not None:
                lat.append(slots[fire] + (answer - start))
                break
            slots[fire] = slots[fire] + (fail - start)
        else:
            lat.append(None)
    return lat


def report(label, lat):
    good = [x for x in lat if x is not None]
    print(
        f"{label}: n={len(good)}/{len(lat)} "
        f"min {min(good)/1000:.0f}s mean {statistics.mean(good)/1000:.0f}s "
        f"max {max(good)/1000:.0f}s stddev {statistics.stdev(good)/1000:.0f}s"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("log")
    ap.add_argument("--to", nargs="+", required=True)
    args = ap.parse_args()
    runs = parse_log(args.log)
    print(f"{args.log}: {len(runs)} requests")
    for target in args.to:
        n, m_max = (int(x) for x in target.split(":"))
        report(target, replay(runs, n, m_max))
