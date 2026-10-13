#!/usr/bin/env bash
# A/B grid bench for the router.
#
# Runs the router locally against the real providers for each requested
# (race_width, max_per_channel) pair, back to back, and saves per run:
#   - the client-side per-request JSON (ttft_s, total_s, status)
#   - a /metrics snapshot taken before shutdown
#   - the router log, which lands in logs/ beside them
# plus a manifest tying the three together and naming the knobs, so any
# saved run can build its own tables without external notes.
#
# Usage: scripts/ab_bench.sh N:M [N:M ...]
#   scripts/ab_bench.sh 1:1 2:2 1:2 2:1
#
# Strips zai from the generated providers.yml like every recorded run so far.
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
BIN=$REPO/target/release/llmrouter
WORK=${AB_BENCH_WORK:-/tmp/llmrouter-ab}
PORT=${AB_BENCH_PORT:-8080}
NREQ=${AB_BENCH_NREQ:-10}

[ $# -ge 1 ] || { echo "usage: $0 N:M [N:M ...]" >&2; exit 2; }

command -v uv >/dev/null || { echo "uv not found" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl not found" >&2; exit 1; }

if [ ! -x "$BIN" ]; then
  echo "building release binaries"
  cargo build --release --bins
fi

# One base config dir (zai stripped, like every recorded run so far), then a
# per-config copy whose router.yml sets the knobs and points the cache, spill
# and log paths at fresh per-run files so learned health and the response
# cache start clean.
mkdir -p "$WORK"
if [ ! -f "$WORK/cfg-base/providers.yml" ]; then
  "$REPO/target/release/llmrouter-make-config" --source "$REPO" --out "$WORK/cfg-base"
  uv run --with pyyaml python - "$WORK/cfg-base/providers.yml" <<'PYEOF'
import sys, yaml
p = sys.argv[1]
doc = yaml.safe_load(open(p))
doc = [x for x in doc if x["name"] != "zai"]
yaml.safe_dump(doc, open(p, "w"), sort_keys=False)
print("providers left:", len(doc))
PYEOF
fi

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUTDIR=$REPO/logs/ab/$STAMP
mkdir -p "$OUTDIR"

for spec in "$@"; do
  RW=${spec%%:*}; MP=${spec##*:}
  rm -rf "$WORK/cfg-$spec" "$WORK/data-$spec"
  cp -r "$WORK/cfg-base" "$WORK/cfg-$spec"
  mkdir -p "$WORK/data-$spec"
  cat > "$WORK/cfg-$spec/router.yml" <<EOF
listen: 0.0.0.0:$PORT
race_width: $RW
max_per_channel: $MP
hedge_delay_ms: 0
streaming: true
spill_path: $WORK/data-$spec/spill
cache:
  path: $WORK/data-$spec/cache.redb
EOF
  echo "== $spec: race_width=$RW max_per_channel=$MP"

  LLMROUTER_CONFIG_DIR="$WORK/cfg-$spec" "$BIN" &
  pid=$!
  trap 'kill -TERM $pid 2>/dev/null || true' EXIT

  ready=0
  for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:$PORT/healthz" 2>/dev/null | grep -q '"eligible":[1-9]'; then
      ready=1; break
    fi
    sleep 2
  done
  if [ "$ready" != 1 ]; then
    echo "catalogue never filled for $spec" >&2
    kill -TERM $pid 2>/dev/null || true; exit 1
  fi

  nonce="bench-$spec-$(date +%s)"
  uv run --with httpx python - "$PORT" "$spec" "$nonce" "$NREQ" <<'PYEOF' | tee "$OUTDIR/$spec.client.json"
import asyncio, json, sys, time
import httpx

port, spec, nonce, n = int(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4])
URL = f"http://127.0.0.1:{port}/v1/chat/completions"

async def one(client, i):
    body = {
        "model": "moonshotai/kimi-k3",
        "messages": [{"role": "user",
                      "content": f"Count from 1 to 50, one number per line. Nonce {nonce}-{i}."}],
        "stream": True,
        "max_tokens": 256,
    }
    t0 = time.monotonic()
    ttft = None
    try:
        async with client.stream("POST", URL, json=body, timeout=1800.0) as r:
            if r.status_code != 200:
                text = (await r.aread()).decode(errors="replace")[:300]
                return {"idx": i, "status": r.status_code, "error": text,
                        "ttft_s": None, "total_s": round(time.monotonic() - t0, 1)}
            async for line in r.aiter_lines():
                if line.startswith("data:"):
                    if ttft is None:
                        ttft = time.monotonic() - t0
                    if line.strip() == "data: [DONE]":
                        break
        return {"idx": i, "status": 200,
                "ttft_s": round(ttft, 2) if ttft is not None else None,
                "total_s": round(time.monotonic() - t0, 1)}
    except Exception as e:
        return {"idx": i, "status": 0, "error": str(e)[:300],
                "ttft_s": None, "total_s": round(time.monotonic() - t0, 1)}

async def main():
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[one(client, i) for i in range(n)])
    print(json.dumps({"spec": spec, "nonce": nonce, "results": results}, indent=1))

asyncio.run(main())
PYEOF

  curl -fsS "http://127.0.0.1:$PORT/metrics" > "$OUTDIR/$spec.metrics.txt" 2>/dev/null || true
  kill -TERM $pid 2>/dev/null || true
  wait $pid 2>/dev/null || true
  trap - EXIT
  log=$(ls -t "$REPO"/logs/*.log 2>/dev/null | grep -v '/\._' | head -1)
  printf '%s client=%s metrics=%s router_log=%s\n' \
    "$spec" "$OUTDIR/$spec.client.json" "$OUTDIR/$spec.metrics.txt" "$log" >> "$OUTDIR/manifest.txt"
  echo "== $spec done"
done

cat "$OUTDIR/manifest.txt"
