//! Integration tier: the real router against wiremock upstreams and a real
//! redb on a tempdir.
//!
//! See tests/README.md. Each module drives the real router over a real socket
//! against wiremock upstreams, with tempfile handing out a throwaway directory.

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
