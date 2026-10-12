//! Integration tier: the real router against wiremock upstreams and a real
//! redb on a tempdir.
//!
//! See tests/README.md. Each module drives the real router over a real socket
//! against wiremock upstreams, with tempfile handing out a throwaway directory.

mod build_providers;
mod cache;
mod health;
mod race;
mod server_edge;
mod startup;

/// A fresh set of counters, for one test.
///
/// `Metrics` owns its recorder rather than installing a process-wide one, so
/// every call here hands back counters nothing else writes to. A test can
/// therefore assert the exact value its own request moved.
pub fn test_metrics() -> std::sync::Arc<llmrouter::Metrics> {
    std::sync::Arc::new(llmrouter::Metrics::new().expect("the prometheus recorder builds"))
}

/// `actions/checkout` materialises tracked content only, so an untracked file
/// under src/ or tests/ builds on the author's machine and fails on the
/// runner: `tests/integration/main.rs` declaring `mod startup;` with
/// `startup.rs` unstaged kills the whole target with "file not found for
/// module". Same guard as the one the smoke tier keeps on `.dockerignore`,
/// widened to the sources. ci.sh checks this too; it lives here as well so a
/// plain `cargo test` catches it.
/// The deploy directories sit inside the main checkout: on the TrueNAS host
/// `/mnt/shuttle/share/app-data/llmrouter` is the working tree, so the live
/// config is `config/` and the `/data` mount is `data/`. `.gitignore` is the
/// only thing keeping the live keys in `config/.env` and the client prompts
/// in `data/spill/` out of git. This pins those rules so nobody can delete
/// them as clutter. `--no-index` checks the rules alone, tracked or not.
#[test]
fn deploy_directories_inside_the_checkout_are_ignored() {
    let paths = [
        "config/.env",
        "config/providers.yml",
        "config/models.yml",
        "config/router.yml",
        "config/intelligence.yml",
        "config/model_capabilities.csv",
        "data/cache.redb",
        "data/spill/prompt.json",
    ];
    let output = std::process::Command::new("git")
        .args(["check-ignore", "--no-index", "--"])
        .args(paths)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("git must be on PATH");

    let ignored = String::from_utf8_lossy(&output.stdout);
    let ignored: Vec<&str> = ignored.lines().collect();
    let exposed: Vec<&str> = paths
        .iter()
        .copied()
        .filter(|p| !ignored.contains(p))
        .collect();
    assert!(
        exposed.is_empty(),
        "these deploy paths sit inside the checkout and .gitignore no longer \
         covers them, so git would see live keys or client prompts: {exposed:?}"
    );
}

#[test]
fn every_source_file_is_tracked_by_git() {
    let output = std::process::Command::new("git")
        .args([
            "ls-files",
            "--others",
            "--exclude-standard",
            "--",
            "src",
            "tests",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("git must be on PATH");
    assert!(output.status.success(), "git ls-files failed");

    let untracked = String::from_utf8_lossy(&output.stdout);
    assert!(
        untracked.trim().is_empty(),
        "these files are not in git, so an Actions checkout will not have them \
         and the build there fails. Run `git add` on:\n{untracked}"
    );
}
