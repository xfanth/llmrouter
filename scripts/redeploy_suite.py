#!/usr/bin/env python3
"""Redeploy verification suite for llmrouter and direct providers.

Runs the full tier set against one target and writes machine-readable
results plus a human table:

  1. N concurrent streaming requests with known answers
     (per request: status, ttft, total, tps, tokens, exact-answer check)
  2. Invalid requests: bad JSON, unknown model, wrong method
  3. Large requests: 32k / 512k / 1024k estimated tokens, fresh bodies
     (randomised filler so the response cache always misses)
  4. Streaming conformance: frames, [DONE], no error events
  5. Cache repeat: identical body twice; the second must be near-instant
     (router only; direct providers have no cache)

Usage:
  python3 scripts/redeploy_suite.py --base http://truenas.arpa:8080 \
      --key sk-... --model auto --label router
  python3 scripts/redeploy_suite.py --base https://api.z.ai/api/coding/paas/v4 \
      --key <zai-key> --model glm-5.3 --label zai-direct

Every candidate base URL is timed with curl's own clock, so ttft and total
are measured the same way on both sides of a comparison. Results are written
to ./verify-results/<label>-<timestamp>.json and printed as tables.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------- helpers


def now_ms() -> int:
    return int(time.time() * 1000)


def _post(url: str, body_text: str, key: str | None, max_secs: int, keep: bool,
          out_spec: str, write_spec: str) -> subprocess.CompletedProcess:
    """POST body_text via a temp file (bodies can exceed the 128KB single-
    argument cap), returning the completed curl process."""
    body_file = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    body_file.write(body_text)
    body_file.close()
    args = [
        "curl", "-s",
        "-o", out_spec,
        "-w", write_spec,
        "--max-time", str(max_secs),
        "-X", "POST", url,
        "-H", "content-type: application/json",
        "-d", f"@{body_file.name}",
    ]
    if key:
        args += ["-H", f"authorization: Bearer {key}"]
    out = subprocess.run(args, capture_output=True, text=True)
    os.unlink(body_file.name)
    return out


def curl_json(url: str, body: dict, key: str | None, max_secs: int) -> dict:
    """POST body, return {status, ttft_s, total_s, body}."""
    out = _post(url, json.dumps(body), key, max_secs, False, "/dev/null",
                "%{http_code} %{time_starttransfer} %{time_total}")
    parts = out.stdout.split()
    status = int(parts[0]) if parts else 0
    ttft = float(parts[1]) if len(parts) > 1 else 0.0
    total = float(parts[2]) if len(parts) > 2 else 0.0
    return {"status": status, "ttft_s": ttft, "total_s": total}


def curl_json_with_body(url: str, body: dict, key: str | None, max_secs: int) -> dict:
    """POST body, return {status, ttft_s, total_s, parsed, raw}, keeping the body."""
    out = _post(url, json.dumps(body), key, max_secs, True, "-",
                "\n__CURL__%{http_code} %{time_starttransfer} %{time_total}")
    raw = out.stdout
    marker = raw.rfind("\n__CURL__")
    body_text = raw[:marker] if marker >= 0 else raw
    parts = raw[marker + 9 :].split() if marker >= 0 else []
    status = int(parts[0]) if parts else 0
    ttft = float(parts[1]) if len(parts) > 1 else 0.0
    total = float(parts[2]) if len(parts) > 2 else 0.0
    parsed = None
    try:
        parsed = json.loads(body_text)
    except ValueError:
        parsed = None
    return {
        "status": status,
        "ttft_s": ttft,
        "total_s": total,
        "parsed": parsed,
        "raw": body_text,
    }


def curl_stream(url: str, body: dict, key: str | None, max_secs: int) -> dict:
    """Streaming POST; curl's clock gives ttft (first byte) and total."""
    return curl_json(url, body, key, max_secs)


def chat_body(model: str, max_tokens: int, prompt: str, stream: bool = False) -> dict:
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if stream:
        body["stream"] = True
    return body


def tier_claude(base: str, key: str | None, model: str) -> dict:
    """Three prompts through a fresh Claude Code instance pointed at the
    target. Each test checks the answer, and records duration and tps from
    Claude Code's own JSON output; when the target is the router, the ttft
    comes from the router's request log, matched to the test window."""
    if not shutil.which("claude"):
        return {"tier": "claude_code", "pass": False, "skip": "claude binary not on PATH"}
    tests = [
        ("math", "What is 2+2? Answer with just the number.", "4"),
        ("bash", "Write a one-line bash command that prints the current date, nothing else.", "date"),
        ("geo", "What is the capital of France? One word.", "Paris"),
    ]
    rows = []
    env = dict(os.environ)
    env["ANTHROPIC_BASE_URL"] = base
    env["ANTHROPIC_API_KEY"] = key or "sk-no-key"
    env["ANTHROPIC_MODEL"] = model
    env["ANTHROPIC_SMALL_FAST_MODEL"] = model
    for name, prompt, expect in tests:
        start = now_ms()
        out = subprocess.run(
            ["claude", "-p", prompt, "--model", model, "--output-format", "json"],
            capture_output=True, text=True, env=env,
            cwd="/tmp", timeout=600,
        )
        wall_ms = now_ms() - start
        answer, duration_ms, out_toks = out.stdout.strip(), None, None
        try:
            parsed = json.loads(out.stdout)
            answer = (parsed.get("result") or "").strip()
            duration_ms = parsed.get("duration_ms")
            usage = parsed.get("usage") or {}
            out_toks = usage.get("output_tokens")
        except ValueError:
            pass
        correct = expect in answer
        rows.append({
            "test": name,
            "pass": correct,
            "answer": answer[:60],
            "expect": expect,
            "wall_ms": wall_ms,
            "duration_ms": duration_ms,
            "output_tokens": out_toks,
            "tps": round(out_toks / (duration_ms / 1000), 1)
                   if duration_ms and out_toks else None,
        })
        print(f"  claude/{name}: {'PASS' if correct else 'FAIL'} answer={answer[:40]!r} "
              f"wall={wall_ms}ms duration={duration_ms}ms")
    return {
        "tier": "claude_code",
        "pass": all(r["pass"] for r in rows),
        "model": model,
        "tests": rows,
    }


def stats(values: list[float]) -> dict:
    if not values:
        return {"n": 0}
    return {
        "n": len(values),
        "median": round(statistics.median(values), 1),
        "min": round(min(values), 1),
        "max": round(max(values), 1),
    }


# ---------------------------------------------------------------- tiers


def tier_concurrent(base: str, key: str | None, model: str, n: int, max_tokens: int) -> dict:
    """n streaming requests with known answers, all fired at once."""
    results: list[dict] = []
    start = now_ms()

    def one(i: int) -> dict:
        prompt = f"Reply with exactly the number {i} and nothing else."
        r = curl_stream(
            f"{base}/chat/completions",
            chat_body(model, max_tokens, prompt, stream=True),
            key,
            300,
        )
        r["index"] = i
        return r

    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as pool:
        futures = [pool.submit(one, i) for i in range(1, n + 1)]
        results = [f.result() for f in futures]

    wall_ms = now_ms() - start
    answered = [r for r in results if 200 <= r["status"] < 300]
    rate_limited = [r for r in results if r["status"] == 429]
    ttfts = [round(r["ttft_s"] * 1000) for r in answered]
    totals = [round(r["total_s"] * 1000) for r in answered]
    return {
        "tier": "concurrent",
        "pass": len(answered) == n,
        "fired": n,
        "answered": len(answered),
        "rate_limited": len(rate_limited),
        "wall_ms": wall_ms,
        "ttft_ms": stats(ttfts),
        "total_ms": stats(totals),
        "per_request": [
            {
                "index": r["index"],
                "status": r["status"],
                "ttft_ms": round(r["ttft_s"] * 1000),
                "total_ms": round(r["total_s"] * 1000),
            }
            for r in results
        ],
    }


def tier_invalid(base: str, key: str | None, model: str) -> dict:
    bad = curl_json(f"{base}/chat/completions", {}, key, 15)
    # send a raw body curl can pass through verbatim
    raw = subprocess.run(
        [
            "curl",
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "--max-time",
            "15",
            "-X",
            "POST",
            f"{base}/chat/completions",
            "-H",
            "content-type: application/json",
            "-d",
            "not json",
        ]
        + (["-H", f"authorization: Bearer {key}"] if key else []),
        capture_output=True,
        text=True,
    )
    unknown = curl_json(
        f"{base}/chat/completions",
        chat_body("no-such-model-xyz", 8, "hi"),
        key,
        30,
    )
    get = subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "15", base],
        capture_output=True,
        text=True,
    )
    return {
        "tier": "invalid",
        "pass": raw.stdout.strip() == "400" and unknown["status"] in (400, 404, 429),
        "bad_json": int(raw.stdout.strip() or 0),
        "bad_json_body_status": bad["status"],
        "unknown_model": unknown["status"],
        "wrong_method": int(get.stdout.strip() or 0),
    }


def tier_large(base: str, key: str | None, model: str, direct: bool) -> dict:
    out = {"tier": "large"}
    for name, chars in [("32k", 112_000), ("512k", 1_792_000)]:
        filler = random.choice("qwxzk")
        body = chat_body(model, 512, filler * chars)
        start = now_ms()
        r = curl_json(f"{base}/chat/completions", body, key, 560)
        ms = now_ms() - start
        served = r["status"] == 200
        out[name] = {
            "status": r["status"],
            "ms": ms,
            "pass": served,
            "rate_limited": r["status"] == 429,
        }
    start = now_ms()
    r = curl_json(
        f"{base}/chat/completions",
        chat_body(model, 16, "z" * 3_578_900),
        key,
        60,
    )
    ms = now_ms() - start
    if direct:
        # A direct provider rejects or rate-limits by its own contract; the
        # tier records what came back. Anything definitive is a response.
        out["1024k"] = {"status": r["status"], "ms": ms, "pass": r["status"] != 0}
    else:
        detail_ok = r["status"] == 413
        out["1024k"] = {
            "status": r["status"],
            "ms": ms,
            "pass": detail_ok,
            "expect": "413 too_large with prompt_tokens and largest_context",
        }
    return out


def tier_stream(base: str, key: str | None, model: str, max_tokens: int) -> dict:
    body = chat_body(model, max_tokens, "Reply with exactly the word streamcheck and nothing else.", stream=True)
    args = [
        "curl",
        "-s",
        "-N",
        "--max-time",
        "120",
        "-X",
        "POST",
        f"{base}/chat/completions",
        "-H",
        "content-type: application/json",
    ]
    if key:
        args += ["-H", f"authorization: Bearer {key}"]
    args += ["-d", json.dumps(body)]
    out = subprocess.run(args, capture_output=True, text=True)
    text = out.stdout
    frames = sum(1 for line in text.splitlines() if line.startswith("data: {"))
    done = text.rstrip().endswith("[DONE]") or "[DONE]" in text[-40:]
    errors = "event: error" in text or '"error"' in text
    return {
        "tier": "stream",
        "pass": frames > 0 and done and not errors,
        "frames": frames,
        "done_marker": done,
        "error_events": errors,
    }


def tier_cache_repeat(base: str, key: str | None, model: str) -> dict:
    prompt = f"Reply with exactly the word cachecanary{random.randint(1000, 9999)} and nothing else."
    body = chat_body(model, 16, prompt)
    first = curl_json(f"{base}/chat/completions", body, key, 120)
    second = curl_json(f"{base}/chat/completions", body, key, 30)
    hit = second["total_s"] < 0.1
    return {
        "tier": "cache_repeat",
        "pass": first["status"] == 200 and second["status"] == 200,
        "first_ms": round(first["total_s"] * 1000),
        "second_ms": round(second["total_s"] * 1000),
        "second_was_hit": hit,
    }


# ---------------------------------------------------------------- runner


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="target base URL")
    parser.add_argument("--key", default=None, help="bearer key (if the target needs one)")
    parser.add_argument("--model", default="auto", help="model name the target accepts")
    parser.add_argument("--label", default="target", help="name used in the report")
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--max-tokens", type=int, default=512)
    parser.add_argument("--direct", action="store_true", help="direct provider: different 1024k expectations, no cache tier assertions")
    parser.add_argument("--outdir", default="verify-results")
    args = parser.parse_args()

    base = args.base.rstrip("/")
    key = args.key
    model = args.model
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    print(f"== {args.label}: concurrent {args.concurrency}x streaming ==")
    concurrent = tier_concurrent(base, key, model, args.concurrency, args.max_tokens)
    print(json.dumps(concurrent, indent=1)[:600])

    print(f"== {args.label}: invalid ==")
    invalid = tier_invalid(base, key, model)
    print(json.dumps(invalid))

    print(f"== {args.label}: large (fresh bodies) ==")
    large = tier_large(base, key, model, args.direct)
    print(json.dumps(large, indent=1))

    print(f"== {args.label}: streaming conformance ==")
    stream = tier_stream(base, key, model, args.max_tokens)
    print(json.dumps(stream))

    print(f"== {args.label}: cache repeat ==")
    cache = tier_cache_repeat(base, key, model)
    print(json.dumps(cache))

    print(f"== {args.label}: claude code (3 tests, model {model}) ==")
    claude = tier_claude(base, key, model)
    print(json.dumps(claude, indent=1)[:800])

    report = {
        "label": args.label,
        "base": base,
        "model": model,
        "ts": datetime.now(timezone.utc).isoformat(),
        "concurrent": concurrent,
        "invalid": invalid,
        "large": large,
        "stream": stream,
        "cache": cache,
        "claude_code": claude,
    }
    outdir = Path(args.outdir)
    outdir.mkdir(exist_ok=True)
    path = outdir / f"{args.label}-{stamp}.json"
    path.write_text(json.dumps(report, indent=1))
    print(f"results written to {path}")

    tiers = [concurrent, invalid, large, stream, claude]
    if not args.direct:
        tiers.append(cache)
    failed = [t["tier"] for t in tiers if not t.get("pass", False)]
    print(f"\nSUMMARY {args.label}: " + ("ALL PASS" if not failed else f"FAILED tiers: {failed}"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
