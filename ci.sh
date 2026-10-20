#!/usr/bin/env bash
# The whole gate. Same checks as GitHub Actions, so a green run here means a
# green run there. The two one-second checks run first and fail fast. Then
# clippy, coverage and the smoke tier run side by side: none depends on
# another, and each has its own target dir so none waits on another's cargo
# lock. There is no separate plain `cargo test`: the coverage run executes the
# same tests, so a pass there proves they pass.
set -euo pipefail

cd "$(dirname "$0")"

# Without this the build writes to a slow network share. The default is keyed
# on this checkout's absolute path, so two worktrees, or two runs of different
# clones, never share a target dir. They would otherwise collide: the coverage
# job starts with `cargo llvm-cov clean --workspace`, which would delete the
# other run's .profraw files mid-flight and fail its 80/80 gate on covered
# code, and both runs would write the same lcov.info.
CHECKOUT_KEY=$(printf %s "$PWD" | cksum | cut -d" " -f1)
BASE_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/llmrouter-target-$CHECKOUT_KEY}"
export PATH="$HOME/.cargo/bin:$PATH"

# Compose's default project name is the basename of the directory holding the
# compose file, not its full path, and the project name decides the image
# name, the container name and the named volume. Two clones both called
# llmrouter therefore share all three: one run's `compose up --build`
# recreates the other's container mid-test, and its `down -v` deletes the
# other's volume. Key the project on the whole path instead. Exported, so the
# smoke test's own compose calls and the cleanup trap below agree on it.
export COMPOSE_PROJECT_NAME="llmrouter-smoke-$CHECKOUT_KEY"

# The gate, on both numbers. Region coverage is the closest thing Rust has to
# branch coverage.
MIN_LINES=80
MIN_REGIONS=80

LCOV_PATH="${BASE_TARGET_DIR}/lcov.info"
mkdir -p "$BASE_TARGET_DIR"

# Stop the background jobs if this script is interrupted, times out or dies
# early. Each job runs in its own process group (see `set -m` below), so
# killing the group reaches cargo, nextest and docker under the job subshell,
# not just the subshell. A smoke run killed part way never reaches its own
# `compose down -v`, so take the stack down here instead.
PIDS=""
cleanup() {
    local pid smoke_killed=""
    for pid in $PIDS; do
        if kill -- "-$pid" 2>/dev/null && [ "$pid" = "${SMOKE_PID:-}" ]; then
            smoke_killed=1
        fi
    done
    if [ -n "$smoke_killed" ]; then
        wait 2>/dev/null || true
        docker compose -f docker-compose.yml -f docker-compose.ci.yml \
            down -v >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

step() { printf '\n==> %s\n' "$1"; }

step "every source file is tracked"
# actions/checkout only materialises tracked files, so anything untracked
# that the build reads (a module, Cargo.lock, rust-toolchain.toml, a compose
# file) works on this laptop and is missing on the runner. Scoped to the paths
# the build actually reads: a repo-wide check also trips on scratch notes,
# editor directories and agent worktrees such as .claude/, which .gitignore
# does not cover and which CI never reads.
untracked=$(git ls-files --others --exclude-standard -- \
    src tests static Cargo.toml Cargo.lock rust-toolchain.toml Dockerfile \
    docker-compose.yml docker-compose.ci.yml)
if [ -n "$untracked" ]; then
    echo "FAIL: these files are not in git, so CI will never see them:"
    echo "$untracked" | sed 's/^/         /'
    echo "         Run: git add <file>"
    exit 1
fi
tracked_secs=$SECONDS

step "cargo fmt --check"
cargo fmt --all -- --check
fmt_secs=$(( SECONDS - tracked_secs ))

# Test tools pinned to the same versions as .github/workflows/ci.yml, so
# nextest and the coverage figures behave the same here and there. A missing
# or different local copy is replaced, not just a missing one. A test,
# the_test_tools_are_pinned_to_the_same_versions_in_ci_sh_and_ci_yml, fails the
# build if these two versions drift from the ones ci.yml installs. The Rust
# toolchain itself is pinned in rust-toolchain.toml.
NEXTEST_VERSION=0.9.148
LLVM_COV_VERSION=0.9.1
ensure_tool() {
    local crate=$1 subcommand=$2 want=$3
    if ! cargo "$subcommand" --version 2>/dev/null | grep -qxE "cargo-$subcommand $want( .*)?"; then
        step "installing $crate $want"
        # Low parallelism at every level keeps the peak memory down: a
        # release build of nextest with every core at once was SIGKILLed by
        # the memory-capped self-hosted runner the organisation runs the
        # gate on. aws-lc-sys drives its own cmake and make, so cargo's job
        # count alone does not bound it.
        CARGO_BUILD_JOBS=2 CMAKE_BUILD_PARALLEL_LEVEL=1 MAKEFLAGS=-j1 \
            cargo install --locked --force --jobs 2 "$crate" --version "$want"
    fi
}
ensure_tool cargo-nextest nextest "$NEXTEST_VERSION"
ensure_tool cargo-llvm-cov llvm-cov "$LLVM_COV_VERSION"

# Each job runs in a background subshell with `set -e` still on. It writes its
# output to its own log, its wall time to <name>.secs and, where a pass is not
# the whole story, a result line to <name>.result.

job_clippy() {
    export CARGO_TARGET_DIR="$BASE_TARGET_DIR/clippy"
    cargo clippy --all-targets -- -D warnings
    # Everything else builds the bins in debug, and the image build only
    # compiles llmrouter, so nothing would otherwise compile
    # src/bin/make-config.rs under [profile.release]. A cfg(not(debug_assertions))
    # block, a monomorphisation-time error or a panic = "abort" incompatibility
    # in it would go green. A check would not catch the last two: it stops at
    # metadata, so it never monomorphises and never links. So build it for real.
    # It is the only bin without a release build elsewhere, and it is small.
    cargo build --release --bin llmrouter-make-config
}

job_coverage() {
    export CARGO_TARGET_DIR="$BASE_TARGET_DIR/coverage"
    # One instrumented nextest run (unit, contract, integration), then two
    # reports off the same profile data: the human summary with the gate, and
    # the lcov file CI keeps as an artefact.
    # --profraw-only clears last run's profile data and nothing else. A bare
    # clean would delete target/llvm-cov-target, i.e. the whole instrumented
    # build, and recompile every dependency from cold.
    cargo llvm-cov clean --workspace --profraw-only
    # clean --profraw-only stops before deleting the merged profile. If a run
    # then produces no profraw, cargo-llvm-cov reports off the leftover
    # profdata and the gate passes on the previous run's figures. Bin it.
    # Glob because the file is named after the checkout directory, not the
    # crate, so it differs between a worktree and the CI checkout.
    rm -f "$CARGO_TARGET_DIR"/llvm-cov-target/*.profdata
    cargo llvm-cov nextest --all-features --workspace --no-report \
        --lib --bins --test contract --test integration
    # macOS writes an AppleDouble `._` sidecar beside every file on an NFS or
    # SMB share. cargo-llvm-cov globs `*.profraw`, picks the sidecars up and
    # llvm-profdata then refuses the whole merge. Only bites when the target
    # dir sits in a checkout on a network share, which is what the runner
    # does, so clear them before the report rather than leaving a trap for
    # anyone who overrides CARGO_TARGET_DIR.
    find "$CARGO_TARGET_DIR" -name '._*.profraw' -delete 2>/dev/null || true
    cargo llvm-cov report --lcov --output-path "$LCOV_PATH"
    # Prints the table, TOTAL row included, before the gate decides, so the
    # closing summary can read the figures from coverage.log either way.
    cargo llvm-cov report --summary-only \
        --fail-under-lines "$MIN_LINES" --fail-under-regions "$MIN_REGIONS"
}

job_smoke() {
    # A runner without a docker daemon cannot build the image: the org's
    # self-hosted TrueNAS runner has no docker socket, by the central
    # platform's security policy. The tier reports itself skipped rather
    # than failing there, and the summary shows why. Anything dockerless
    # therefore leans on the local gate, which is where the smoke tier
    # is meant to run anyway.
    if ! docker info > /dev/null 2>&1; then
        echo "skipped (no docker)" > "$LOG_DIR/smoke.result"
        return 0
    fi
    export CARGO_TARGET_DIR="$BASE_TARGET_DIR/smoke"
    # CI=1 is what GitHub Actions sets, and the image test demands a working
    # docker daemon when it is set rather than skipping. Setting it here keeps
    # the local gate as strict as the runner, so a green ci.sh really does
    # predict a green CI. The skip path still exists for anyone running
    # `cargo test --test smoke` by hand.
    CI=1 cargo test --test smoke -- --nocapture
}

# macOS leaves an AppleDouble `._` sidecar beside every file it edits on an
# NFS or SMB share. BuildKit reads extended attributes on everything it walks,
# the share refuses that on a sidecar, and the smoke tier's image build dies
# before .dockerignore is consulted. They are git-ignored and never exist on
# the runner, so sweep them here, before the jobs start. Not inside the smoke
# job: with the target dir in the checkout, that sweep deletes sidecars that
# clippy and coverage are writing at the same moment, and their build scripts
# fail with "Operation not permitted".
find . -name '._*' -type f -not -path './.git/*' -delete 2>/dev/null || true

LOG_DIR=$(mktemp -d "${TMPDIR:-/tmp}/llmrouter-ci.XXXXXX")
JOBS="clippy coverage smoke"
step "clippy, coverage and smoke tier, in parallel (logs in $LOG_DIR)"
# Job control puts each background job in its own process group, led by the
# job's PID, which is what lets cleanup() kill the whole tree.
set -m
for name in $JOBS; do
    # No `||` or `if` around the job: either would switch `set -e` off inside
    # it, and a failed test run would then fall through to a green report.
    (
        SECONDS=0
        trap 'echo $SECONDS > "$LOG_DIR/$name.secs"' EXIT
        "job_$name" > "$LOG_DIR/$name.log" 2>&1
    ) &
    PIDS="$PIDS $!"
    [ "$name" != smoke ] || SMOKE_PID=$!
done
set +m

failed=""
set -- $PIDS
for name in $JOBS; do
    pid=$1; shift
    if wait "$pid"; then
        [ -f "$LOG_DIR/$name.result" ] || echo "pass" > "$LOG_DIR/$name.result"
        echo "    $name: pass"
    else
        echo "FAIL" > "$LOG_DIR/$name.result"
        echo "    $name: FAIL"
        failed="$failed $name"
    fi
done

for name in $failed; do
    printf '\n==> %s FAILED, last 80 lines of %s\n' "$name" "$LOG_DIR/$name.log"
    tail -n 80 "$LOG_DIR/$name.log"
done

# TOTAL row of the llvm-cov summary: column 4 is region cover, 10 is line.
read -r region_pct line_pct < <(awk '$1 == "TOTAL" { print $4, $10 }' "$LOG_DIR/coverage.log" | tr -d %) || true

row() { printf '%-22s %-24s %s\n' "$1" "$2" "$3"; }
secs() { if [ -f "$LOG_DIR/$1.secs" ]; then echo "$(cat "$LOG_DIR/$1.secs")s"; fi; }
result() { cat "$LOG_DIR/$1.result"; }

printf '\n'
row "step" "result" "time"
row "----------------------" "------------------------" "-----"
row "sources tracked" "pass" "${tracked_secs}s"
row "cargo fmt --check" "pass" "${fmt_secs}s"
row "cargo clippy" "$(result clippy)" "$(secs clippy)"
row "coverage (tests)" "$(result coverage)" "$(secs coverage)"
row "smoke tier" "$(result smoke)" "$(secs smoke)"
row "line coverage" "${line_pct:-n/a}% (min ${MIN_LINES}%)" ""
row "region coverage" "${region_pct:-n/a}% (min ${MIN_REGIONS}%)" ""
row "total wall clock" "" "${SECONDS}s"
row "lcov report" "$LCOV_PATH" ""
printf '\n'

[ -z "$failed" ] || exit 1
rm -rf "$LOG_DIR"
