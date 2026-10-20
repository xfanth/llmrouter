//! Bootstrap.
//!
//! jemalloc, logging, config, shared state, the probe waves in the background, axum,
//! and a graceful shutdown on SIGTERM that flushes the health table.
//!
//! Startup never blocks on an upstream. The port binds and serves with zero
//! channels; the catalogue fills itself behind the server.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use llmrouter::{
    Admission, Cache, Catalogue, Config, HealthTable, Metrics, NetTest, Prober, Racer, Upstream,
};
use tokio_util::sync::CancellationToken;

// glibc's malloc fragments badly on this allocation pattern: a 3.8MB JSON
// document parsed per client request across many threads. The 15% fragmentation
// figure in the resource budget assumes jemalloc.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// What the `503` from the memory gate puts in its `Retry-After`. DESIGN.md
/// names no figure for it: it only says the gate rejects rather than queues and
/// lets the client decide when to come back.
const RSS_RETRY_AFTER: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Held for the life of the process: dropping it flushes the file writer.
    let (log_setup, _log_guard) = llmrouter::logging::init().context("opening the log file")?;

    let dir = config_dir();
    let config = Arc::new(
        Config::load(&dir).with_context(|| format!("loading config from {}", dir.display()))?,
    );
    tracing::info!(
        log_file = %log_setup.path.display(),
        config_dir = %dir.display(),
        providers = config.providers.len(),
        ranked_models = config.intelligence.len(),
        capabilities = config.capabilities.len(),
        race_width = config.router.race_width,
        max_per_channel = config.router.max_per_channel,
        hedge_delay_ms = config.router.hedge_delay_ms,
        "config loaded"
    );

    let state = build_state(Arc::clone(&config))?;
    let shutdown = CancellationToken::new();

    // The health table and the catalogue fill themselves behind the server, so
    // the port is listening before the first provider has answered anything.
    let prober = Arc::new(build_prober(&config, &state));
    tokio::spawn({
        let prober = Arc::clone(&prober);
        let shutdown = shutdown.clone();
        async move {
            prober.run_startup_waves().await;
            prober.poll_loop(shutdown).await;
        }
    });
    tokio::spawn(
        Arc::clone(&state.health).flush_loop(config.router.cache.flush_interval, shutdown.clone()),
    );
    tokio::spawn(Arc::clone(&state.history).sampler((*state.metrics).clone(), shutdown.clone()));

    let listener = tokio::net::TcpListener::bind(config.router.listen)
        .await
        .with_context(|| format!("binding {}", config.router.listen))?;
    tracing::info!(listen = %config.router.listen, "serving");

    let result = axum::serve(listener, llmrouter::server::router(Arc::clone(&state)))
        .with_graceful_shutdown(shutdown_signal())
        .await;

    // Stop the poller and the flush loop, then flush once by hand so the
    // learned health survives the restart even if the loop was mid-tick.
    shutdown.cancel();
    if let Err(e) = state.health.flush() {
        tracing::warn!(error = %e, "health flush on shutdown failed");
    }
    if let Err(e) = state.history.flush() {
        tracing::warn!(error = %e, "history flush on shutdown failed");
    }
    state.history.save_counters_from(&state.metrics);
    result.context("server")?;
    tracing::info!("stopped");
    Ok(())
}

/// The prober over the same client as the races.
///
/// It clones the racer's [`Upstream`] rather than building one: a second
/// client would open a second connection pool to the very same provider
/// hosts, and those idle sockets are never freed for the life of the process.
fn build_prober(config: &Arc<Config>, state: &llmrouter::AppState) -> Prober {
    Prober::new(
        Arc::clone(config),
        state.racer.upstream().clone(),
        Arc::clone(&state.catalogue),
        Arc::clone(&state.health),
    )
}

/// Opens the one redb file and builds everything that hangs off it: the health
/// table, the cache, the client, the catalogue, the racer and the gate.
fn build_state(config: Arc<Config>) -> anyhow::Result<Arc<llmrouter::AppState>> {
    let router = &config.router;

    if let Some(parent) = router.cache.path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    // A fresh process owns no spill files, so anything still in there is an
    // orphan from a run that died before PromptBody::drop could delete it
    // (SIGKILL, the OOM killer, or a panic under panic="abort"). Each one is a
    // full client prompt in plaintext on an exported share, so sweep the
    // directory before the port binds rather than letting them pile up.
    match std::fs::remove_dir_all(&router.spill_path) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => {
            return Err(e).with_context(|| format!("sweeping {}", router.spill_path.display()));
        }
    }
    std::fs::create_dir_all(&router.spill_path)
        .with_context(|| format!("creating {}", router.spill_path.display()))?;

    // One redb file holds both tables, so it is opened once and shared.
    // The page cache is capped here, not left at redb's 1GiB default.
    let db = Arc::new(
        llmrouter::cache::open_database(&router.cache)
            .with_context(|| format!("opening {}", router.cache.path.display()))?,
    );

    // Built before the health table and the cache, because both of them
    // record through it. Each `Metrics` owns its recorder rather than
    // installing a process-wide one, so the clones below all write to this
    // set of counters and a test can hold its own.
    let metrics = Arc::new(Metrics::new()?);

    let health = Arc::new(HealthTable::open(
        Arc::clone(&db),
        router.health_poll.retry_base,
        router.health_poll.backoff_max,
        (*metrics).clone(),
    )?);
    let cache = if router.cache.enabled {
        Some(Arc::new(Cache::open(
            db,
            &router.cache,
            (*metrics).clone(),
        )?))
    } else {
        None
    };

    let upstream = Upstream::new(router)?;
    // net.rs takes its timeout from the caller rather than hard-coding one.
    let net = NetTest::new(
        upstream.client().clone(),
        router.health_poll.network_test_timeout,
    );
    let catalogue = Arc::new(Catalogue::new());

    let racer = Arc::new(Racer::new(
        Arc::clone(&config),
        Arc::clone(&catalogue),
        Arc::clone(&health),
        upstream,
        net,
        Arc::clone(&metrics),
    ));

    // The history file sits beside cache.redb, so it lands on the same
    // mounted volume and the dashboard's charts survive a restart.
    let mut history_path = router.cache.path.clone();
    history_path.set_file_name("history.redb");
    let history = Arc::new(llmrouter::history::History::open(&history_path));

    // The counters the previous run saved come back on top of the fresh
    // recorder, so the dashboard's cumulative charts survive a restart.
    let saved = history.load_counters();
    if !saved.is_empty() {
        tracing::info!(series = saved.len(), "restoring saved counters");
        metrics.set_counter_offsets(&saved);
    }

    Ok(Arc::new(llmrouter::AppState {
        admission: Admission::new(router.memory_high_water, RSS_RETRY_AFTER),
        config: Arc::clone(&config),
        catalogue,
        health,
        cache,
        racer,
        metrics,
        history,
        reqlog: Arc::new(llmrouter::reqlog::RequestLog::new()),
        started_at: Instant::now(),
    }))
}

/// Where the six config files live. `LLMROUTER_CONFIG_DIR`, or `/config`.
fn config_dir() -> PathBuf {
    std::env::var_os("LLMROUTER_CONFIG_DIR").map_or_else(|| PathBuf::from("/config"), PathBuf::from)
}

/// Resolves when SIGTERM or ctrl-c arrives. Docker sends SIGTERM, so that is
/// the one that matters in production.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "ctrl-c handler failed");
            // Never resolve, or the server would stop the moment it was built.
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "SIGTERM handler failed");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("ctrl-c received, shutting down"),
        _ = terminate => tracing::info!("SIGTERM received, shutting down"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_dir_falls_back_to_the_container_path() {
        // std::env::set_var is process-wide and the other test runs beside
        // this one, so only the fallback half is asserted on here.
        assert_eq!(
            std::env::var_os("LLMROUTER_CONFIG_DIR").is_none(),
            config_dir() == std::path::Path::new("/config")
        );
    }

    /// The one seam this file owns: a redb file opened once and handed to both
    /// the health table and the cache, with every other piece hanging off it.
    ///
    /// `Metrics::new` installs a process-wide recorder, so this is the only
    /// test in this target that may call `build_state`.
    #[test]
    fn build_state_opens_one_store_and_wires_everything_to_it() {
        let tmp = tempfile::tempdir().expect("a throwaway directory");
        let dir = tmp.path();

        let mut config = Config {
            router: llmrouter::RouterConfig::default(),
            providers: Vec::new(),
            models: Vec::new(),
            intelligence: Vec::new(),
            keys: std::collections::HashMap::new(),
            capabilities: std::collections::HashMap::new(),
        };
        config.router.cache.path = dir.join("cache.redb");
        config.router.spill_path = dir.join("spill");

        // An orphan left by a run that was killed before its destructor ran.
        std::fs::create_dir_all(dir.join("spill")).expect("the spill directory is created");
        let orphan = dir.join("spill").join("orphan.json");
        std::fs::write(&orphan, b"{}").expect("the orphan is written");

        let state = build_state(Arc::new(config)).expect("state builds");

        assert!(
            !orphan.exists(),
            "boot must sweep spill files left by a crashed run"
        );

        assert!(state.cache.is_some(), "cache.enabled defaults to true");
        assert_eq!(state.cache.as_ref().unwrap().entry_count().unwrap(), 0);
        assert_eq!(state.catalogue.len(), 0, "nothing probed yet");
        assert!(state.health.in_backoff(0).is_empty());
        assert!(dir.join("spill").is_dir(), "the spill directory is created");

        // The prober shares the racer's client, so there is one connection
        // pool per host for the whole process, not one per component.
        let prober = build_prober(&state.config, &state);
        assert!(
            prober.upstream().shares_client(state.racer.upstream()),
            "the prober must reuse the racer's client"
        );

        drop(state);
    }

    /// The compose file is the only thing that caps the container at 4GB and
    /// lifts the descriptor limit. Without it the budget is a comment, and the
    /// process grows into the memory the rest of the box needs.
    #[test]
    fn the_compose_file_pins_the_resource_limits_and_the_mounts() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        assert!(root.join("Dockerfile").is_file(), "Dockerfile is missing");

        let text = std::fs::read_to_string(root.join("docker-compose.yml"))
            .expect("docker-compose.yml is missing");
        let compose: serde_yaml::Value = serde_yaml::from_str(&text).expect("compose parses");
        let svc = &compose["services"]["llmrouter"];

        assert_eq!(svc["mem_limit"].as_str(), Some("4g"));
        assert_eq!(svc["ulimits"]["nofile"].as_u64(), Some(65536));
        assert_eq!(svc["restart"].as_str(), Some("unless-stopped"));

        // config_dir() defaults to /config and the cache and spill paths
        // default under /data, so both mounts have to exist, and /config has
        // to be read-only.
        let mounts: Vec<&str> = svc["volumes"]
            .as_sequence()
            .expect("volumes")
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert!(
            mounts.iter().any(|m| m.ends_with(":/config:ro")),
            "/config must be mounted read-only: {mounts:?}"
        );
        assert!(
            mounts.iter().any(|m| m.ends_with(":/data")),
            "/data must be mounted read-write: {mounts:?}"
        );

        let check = &svc["healthcheck"];
        assert!(
            check["test"]
                .as_sequence()
                .expect("healthcheck test")
                .iter()
                .any(|v| v.as_str() == Some("http://localhost:8080/healthz")),
            "the health check must hit /healthz"
        );
        assert_eq!(check["start_period"].as_str(), Some("10s"));
    }

    /// ci.yml is a thin caller of the organisation's shared Rust gate: one
    /// job, no runners chosen here, no tooling installed here, no caching of
    /// its own. Anything the caller adds beyond the call is a second opinion
    /// that drifts from the platform.
    #[test]
    fn the_ci_workflow_is_a_thin_caller_of_the_shared_gate() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let text = std::fs::read_to_string(root.join(".github/workflows/ci.yml"))
            .expect(".github/workflows/ci.yml is missing");
        let ci: serde_yaml::Value = serde_yaml::from_str(&text).expect("ci.yml parses");
        let jobs = ci["jobs"].as_mapping().expect("jobs");
        assert_eq!(
            jobs.len(),
            1,
            "the caller carries exactly one job: the shared gate"
        );
        for (_, job) in jobs {
            let uses = job["uses"]
                .as_str()
                .expect("the job is a call of the shared workflow");
            assert!(
                uses.starts_with("xfanth/.github/.github/workflows/rust-quality.yml@"),
                "the caller calls the organisation's Rust gate: {uses}"
            );
            assert!(
                job.get("steps").is_none() && job.get("runs-on").is_none(),
                "the caller adds no steps and picks no runner"
            );
        }
        for leftover in [
            "taiki-e/install-action",
            "Swatinem/rust-cache",
            "runs-on: ubuntu-latest",
        ] {
            assert!(
                !text.contains(leftover),
                "the caller still carries a piece of the old workflow: {leftover}"
            );
        }
    }

    /// Every test reads files from all over the repo, so nothing may gate
    /// CI on changed paths: a filter that skips a run is a filter that
    /// eventually skips a broken change. The caller runs on every push and
    /// every pull request, and the shared gate runs everything.
    #[test]
    fn the_ci_workflow_runs_on_every_push_and_pull_request() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let text = std::fs::read_to_string(root.join(".github/workflows/ci.yml"))
            .expect(".github/workflows/ci.yml is missing");
        assert!(
            text.contains("pull_request:")
                && text.contains("push:")
                && text.contains("workflow_dispatch:"),
            "the caller triggers on pull requests, master pushes and manual runs: {text}"
        );
        assert!(
            !text.contains("paths:"),
            "the caller must not gate the run on changed paths"
        );
    }

    /// ci.sh pins and installs the test runner and the coverage instrumenter
    /// itself, from their projects' prebuilt releases. ci.yml is a caller of
    /// the organisation's shared gate and installs nothing: a second
    /// installer there would drift from ci.sh's pins.
    #[test]
    fn the_test_tools_are_pinned_in_ci_sh_and_ci_yml_installs_nothing() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let sh = std::fs::read_to_string(root.join("ci.sh")).expect("ci.sh is missing");
        let version = |name: &str| {
            sh.lines()
                .find_map(|l| l.trim().strip_prefix(&format!("{name}=")))
                .unwrap_or_else(|| panic!("ci.sh must set {name}"))
                .trim()
                .to_string()
        };
        let nextest = version("NEXTEST_VERSION");
        let llvm_cov = version("LLVM_COV_VERSION");
        // The pins are real version numbers, not placeholders.
        for version in [&nextest, &llvm_cov] {
            let digits = version.chars().filter(|c| c.is_ascii_digit()).count();
            assert!(digits >= 3, "a plausible pinned version, got {version}");
        }

        let text = std::fs::read_to_string(root.join(".github/workflows/ci.yml"))
            .expect(".github/workflows/ci.yml is missing");
        assert!(
            !text.contains("taiki-e/install-action"),
            "ci.yml must not install the test tools; ci.sh alone does"
        );
        assert!(
            text.contains("xfanth/.github/.github/workflows/rust-quality.yml@"),
            "ci.yml calls the organisation's shared Rust gate"
        );
    }

    /// Scratch directories named after the process id collide between runs: a
    /// file left behind by a failed or killed run is still there when a later
    /// run draws the same pid, and an unrelated test then fails on it. The
    /// production spill path is configured, so no module needs `temp_dir`.
    #[test]
    fn no_module_builds_its_own_scratch_directory() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders = Vec::new();
        for entry in std::fs::read_dir(&src).expect("src is readable") {
            let path = entry.expect("a directory entry").path();
            // The needle is split so this guard does not match itself. A file
            // that is not UTF-8 (a macOS sidecar on a network share) is not a
            // module, so it reads as empty rather than failing the test.
            if path.extension().is_some_and(|e| e == "rs")
                && std::fs::read_to_string(&path)
                    .unwrap_or_default()
                    .contains(concat!("env::temp", "_dir()"))
            {
                offenders.push(path.file_name().unwrap().to_string_lossy().into_owned());
            }
        }
        offenders.sort();
        assert!(
            offenders.is_empty(),
            "use tempfile::tempdir() instead of a hand-rolled scratch directory in: {offenders:?}"
        );
    }
}
