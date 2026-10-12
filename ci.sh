#!/usr/bin/env bash
# The whole gate, in the order a reviewer wants it: format, lint, tests, then
# coverage. Same script locally and in GitHub Actions, so a green run here
# means a green run there.
set -euo pipefail

cd "$(dirname "$0")"

# Without this the build writes to a slow network share.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/llmrouter-target}"
export PATH="$HOME/.cargo/bin:$PATH"

# The gate, on both numbers. Region coverage is the closest thing Rust has to
# branch coverage.
MIN_LINES=80
MIN_REGIONS=80

LCOV_PATH="${CARGO_TARGET_DIR}/lcov.info"

tracked_result="skipped"
fmt_result="skipped"
clippy_result="skipped"
test_result="skipped"
smoke_result="skipped"
line_pct="n/a"
region_pct="n/a"

step() { printf '\n==> %s\n' "$1"; }

step "every source file is tracked"
# actions/checkout only materialises tracked files, so an untracked module
# under src/ or tests/ compiles on this laptop and breaks on the runner.
untracked=$(git ls-files --others --exclude-standard -- src tests)
if [ -n "$untracked" ]; then
    echo "FAIL: these files are not in git, so CI will never see them:"
    echo "$untracked" | sed 's/^/         /'
    echo "         Run: git add <file>"
    exit 1
fi
tracked_result="pass"

step "cargo fmt --check"
cargo fmt --all -- --check
fmt_result="pass"

step "cargo clippy --all-targets -- -D warnings"
cargo clippy --all-targets -- -D warnings
clippy_result="pass"

step "cargo test (unit, contract, integration)"
cargo test --lib --bins --test contract --test integration
test_result="pass"

step "smoke tier"
if docker info >/dev/null 2>&1; then
    cargo test --test smoke -- --nocapture
    smoke_result="pass"
else
    echo "SKIPPED: no working docker daemon on this machine."
    echo "         The smoke tier runs in GitHub Actions on an ubuntu runner."
    smoke_result="skipped (no docker)"
fi

step "coverage"
# One instrumented run, then two reports off the same profile data: the human
# summary with the gate, and the lcov file CI keeps as an artefact.
cargo llvm-cov clean --workspace
cargo llvm-cov --all-features --workspace --no-report \
    --lib --bins --test contract --test integration
cargo llvm-cov report --lcov --output-path "$LCOV_PATH"

# The gate must not jump the summary table, so take its exit code by hand.
coverage_result="pass"
if ! cargo llvm-cov report --summary-only \
    --fail-under-lines "$MIN_LINES" --fail-under-regions "$MIN_REGIONS"; then
    coverage_result="FAIL"
fi

percentages=$(cargo llvm-cov report --json --summary-only | python3 -c '
import json, sys
totals = json.load(sys.stdin)["data"][0]["totals"]
print("%.2f %.2f" % (totals["lines"]["percent"], totals["regions"]["percent"]))
')
line_pct=${percentages% *}
region_pct=${percentages#* }

printf '\n'
printf '%-22s %s\n' "step" "result"
printf '%-22s %s\n' "----------------------" "----------------------"
printf '%-22s %s\n' "sources tracked" "$tracked_result"
printf '%-22s %s\n' "cargo fmt --check" "$fmt_result"
printf '%-22s %s\n' "cargo clippy" "$clippy_result"
printf '%-22s %s\n' "cargo test" "$test_result"
printf '%-22s %s\n' "smoke tier" "$smoke_result"
printf '%-22s %s\n' "line coverage" "${line_pct}% (min ${MIN_LINES}%)"
printf '%-22s %s\n' "region coverage" "${region_pct}% (min ${MIN_REGIONS}%)"
printf '%-22s %s\n' "coverage gate" "$coverage_result"
printf '%-22s %s\n' "lcov report" "$LCOV_PATH"
printf '\n'

[ "$coverage_result" = "pass" ] || exit 1
