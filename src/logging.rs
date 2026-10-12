//! Console and file logging, progress bars and the startup summary table.
//!
//! One subscriber carries three layers: a pretty coloured console layer, a
//! pretty uncoloured file layer behind `tracing-appender`'s non-blocking
//! writer, and `tracing-indicatif`'s layer so a progress bar and a log line do
//! not overwrite each other.
//!
//! Everything bounded at startup gets a bar: the two probe waves and the
//! catalogue build. `indicatif` hides a bar on its own when stderr is not a
//! terminal, so Docker logs stay free of control characters.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use comfy_table::{presets, Table};
use indicatif::{ProgressBar, ProgressStyle};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Where the per-run log files go, relative to the working directory.
const LOG_DIR: &str = "logs";

/// The default filter when `RUST_LOG` says nothing: the router at info and
/// everything noisier at warn.
const DEFAULT_FILTER: &str = "llmrouter=info,warn";

/// The choices [`init`] makes before it builds anything, split out so a test
/// without a terminal can check them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSetup {
    /// The file this run writes to, `./logs/<iso_datetime>.log`.
    pub path: PathBuf,
    /// Colour on the console, on only when stderr really is a terminal.
    pub console_ansi: bool,
    /// Colour in the file, always off.
    pub file_ansi: bool,
}

impl LogSetup {
    /// Decides the log path from the clock and the colour from the terminal.
    pub fn new() -> Self {
        Self {
            path: log_path(Path::new(LOG_DIR), OffsetDateTime::now_utc()),
            console_ansi: std::io::stderr().is_terminal(),
            file_ansi: false,
        }
    }
}

impl Default for LogSetup {
    fn default() -> Self {
        Self::new()
    }
}

/// `<dir>/<iso_datetime>.log`, with the colons swapped for hyphens so the name
/// is usable on every filesystem.
pub fn log_path(dir: &Path, at: OffsetDateTime) -> PathBuf {
    let stamp = at
        .format(&Rfc3339)
        .unwrap_or_else(|_| at.unix_timestamp().to_string())
        .replace(':', "-");
    dir.join(format!("{stamp}.log"))
}

/// Installs the subscriber. The returned guard flushes the file writer when it
/// drops, so `main` has to hold it for the life of the process.
///
/// Returns the setup it used, so the caller can say where the log went.
pub fn init() -> std::io::Result<(LogSetup, tracing_appender::non_blocking::WorkerGuard)> {
    let setup = LogSetup::new();
    let file = create_log_file(&setup.path)?;
    let (writer, guard) = tracing_appender::non_blocking(file);

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));
    let indicatif = tracing_indicatif::IndicatifLayer::new();

    let console = tracing_subscriber::fmt::layer()
        .pretty()
        .with_ansi(setup.console_ansi)
        // Log lines go through the indicatif writer, which parks the bars for
        // the moment it takes to print, rather than letting the two interleave.
        .with_writer(indicatif.get_stderr_writer());
    let to_file = tracing_subscriber::fmt::layer()
        .pretty()
        .with_ansi(setup.file_ansi)
        .with_writer(writer);

    tracing_subscriber::registry()
        .with(filter)
        .with(console)
        .with(to_file)
        .with(indicatif)
        .init();

    Ok((setup, guard))
}

/// Creates the parent directory and opens the file for this run.
pub fn create_log_file(path: &Path) -> std::io::Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::File::create(path)
}

/// A bar over `len` units of bounded work, carrying a rate and an ETA.
///
/// Hidden on its own when stderr is not a terminal.
pub fn bar(len: u64, label: &'static str) -> ProgressBar {
    let style =
        ProgressStyle::with_template("{msg:28} [{bar:30}] {pos}/{len} ({per_sec}, eta {eta})")
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("=> ");
    ProgressBar::new(len).with_style(style).with_message(label)
}

/// The counts a run has after the startup waves.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StartupSummary {
    /// Providers in the config.
    pub providers: usize,
    /// Providers that answered the keyless reachability probe.
    pub providers_reachable: usize,
    /// Provider/key pairs in the config, across the reachable providers.
    pub keys: usize,
    /// Pairs whose keyed `/models` probe answered.
    pub keys_answered: usize,
    /// Channel rows the catalogue ended up with.
    pub channels: usize,
    /// Health entries still waiting out a backoff.
    pub in_backoff: usize,
}

impl StartupSummary {
    /// The summary as a table, ready to log as one block.
    pub fn table(&self) -> String {
        let mut table = Table::new();
        table
            .load_style(presets::UTF8_FULL)
            .set_header(vec!["item", "count"]);
        for (item, count) in [
            ("providers", self.providers),
            ("providers reachable", self.providers_reachable),
            ("keys", self.keys),
            ("keys answered", self.keys_answered),
            ("channels", self.channels),
            ("in backoff", self.in_backoff),
        ] {
            table.add_row(vec![item.to_string(), count.to_string()]);
        }
        table.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_log_path_is_the_run_start_in_iso_form_under_the_log_directory() {
        let at = OffsetDateTime::from_unix_timestamp(1_760_000_000).expect("valid instant");
        let path = log_path(Path::new("logs"), at);

        assert_eq!(path, Path::new("logs/2025-10-09T08-53-20Z.log"));
        assert_eq!(path.extension().and_then(|e| e.to_str()), Some("log"));
        assert!(
            !path.to_string_lossy().contains(':'),
            "colons are not portable in a filename: {}",
            path.display()
        );
    }

    #[test]
    fn two_runs_a_second_apart_do_not_share_a_file() {
        let first = OffsetDateTime::from_unix_timestamp(1_760_000_000).unwrap();
        let second = OffsetDateTime::from_unix_timestamp(1_760_000_001).unwrap();
        assert_ne!(
            log_path(Path::new("logs"), first),
            log_path(Path::new("logs"), second)
        );
    }

    #[test]
    fn opening_the_log_creates_the_directory_when_it_is_missing() {
        let tmp = tempfile::tempdir().expect("a throwaway directory");

        let path = log_path(&tmp.path().join("logs"), OffsetDateTime::now_utc());
        assert!(
            !path.parent().unwrap().exists(),
            "the directory starts missing"
        );

        create_log_file(&path).expect("the log file opens");
        assert!(path.is_file(), "the file exists after opening it");
    }

    #[test]
    fn the_file_layer_never_gets_colour_and_the_console_follows_the_terminal() {
        let setup = LogSetup::new();
        assert!(!setup.file_ansi, "ANSI in the file would be noise");
        assert_eq!(
            setup.console_ansi,
            std::io::stderr().is_terminal(),
            "colour on the console is on exactly when stderr is a terminal"
        );
        assert!(setup.path.starts_with(LOG_DIR));
    }

    #[test]
    fn a_bar_is_hidden_when_stderr_is_not_a_terminal() {
        let bar = bar(10, "wave 1");
        assert_eq!(
            bar.is_hidden(),
            !std::io::stderr().is_terminal(),
            "a bar drawn into a pipe would fill Docker logs with control characters"
        );
        bar.inc(3);
        assert_eq!(bar.position(), 3);
    }

    #[test]
    fn the_summary_table_carries_every_count() {
        let summary = StartupSummary {
            providers: 7,
            providers_reachable: 6,
            keys: 47,
            keys_answered: 44,
            channels: 3336,
            in_backoff: 3,
        };
        let table = summary.table();

        for expected in [
            "providers",
            "keys answered",
            "channels",
            "in backoff",
            "3336",
            "44",
        ] {
            assert!(
                table.contains(expected),
                "{expected} is missing from:\n{table}"
            );
        }
    }
}
