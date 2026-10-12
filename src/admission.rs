//! The admission gate. One gate, and it is RSS.
//!
//! No limit on requests per second and no limit on concurrent requests. Before
//! admitting a client request, read the process RSS and compare it to
//! `memory_high_water`. Over it, `503` with `Retry-After` and the current RSS.
//!
//! Read on every admission decision, never on a timer. One open, one read, one
//! parse is microseconds against a request that lasts minutes, and a cached
//! sample would let requests slip in between reads for no gain.

use std::time::Duration;

/// The gate.
#[derive(Debug, Clone, Copy)]
pub struct Admission {
    high_water: u64,
    retry_after: Duration,
}

/// What the gate decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    /// Under the high water mark. Carries the sample so the request can log it.
    Ok {
        /// Resident set size in bytes at the moment of the decision.
        rss: u64,
    },
    /// Over the high water mark. The `503` body carries all three numbers.
    Reject {
        /// Resident set size in bytes at the moment of the decision.
        rss: u64,
        /// `memory_high_water` from `router.yml`.
        high_water: u64,
        /// What goes in the `Retry-After` header.
        retry_after: Duration,
    },
}

impl Admission {
    /// Builds the gate from `memory_high_water`.
    pub fn new(high_water: u64, retry_after: Duration) -> Self {
        Self {
            high_water,
            retry_after,
        }
    }

    /// Reads RSS and decides. Called once per client request, before anything
    /// else happens.
    ///
    /// If the RSS read itself fails the gate opens. A router that cannot read
    /// its own memory is still a working router, and rejecting every request
    /// because of an unreadable file would be a worse failure than the one the
    /// gate guards against.
    pub fn check(&self) -> Admit {
        let rss = match read_rss_bytes() {
            Ok(rss) => rss,
            Err(e) => {
                tracing::warn!(error = %e, "could not read RSS, admitting");
                return Admit::Ok { rss: 0 };
            }
        };
        self.decide(rss)
    }

    /// The comparison on its own, with the sample handed in. Split out so the
    /// boundary behaviour can be tested without a process to measure.
    fn decide(&self, rss: u64) -> Admit {
        if rss > self.high_water {
            Admit::Reject {
                rss,
                high_water: self.high_water,
                retry_after: self.retry_after,
            }
        } else {
            Admit::Ok { rss }
        }
    }

    /// The configured ceiling, for `/metrics`.
    pub fn high_water(&self) -> u64 {
        self.high_water
    }
}

/// Resident set size of this process, in bytes.
///
/// On Linux this is the second field of `/proc/self/statm`, in pages,
/// multiplied by the page size.
#[cfg(target_os = "linux")]
pub fn read_rss_bytes() -> std::io::Result<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm")?;
    Ok(parse_statm_resident(&statm)?.saturating_mul(page_size()))
}

/// Resident set size of this process, in bytes.
///
/// On macOS there is no `/proc`. The design asks for the `task_info` Mach
/// call, which needs a crate this build does not carry, so the fallback asks
/// `ps` for the same number instead. `ps` reports RSS in kibibytes. This is a
/// development convenience only: the router ships in a Debian container, where
/// the Linux path above is the one that runs.
#[cfg(not(target_os = "linux"))]
pub fn read_rss_bytes() -> std::io::Result<u64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p"])
        .arg(std::process::id().to_string())
        .output()?;
    parse_ps_rss_kib(&String::from_utf8_lossy(&out.stdout)).map(|kib| kib.saturating_mul(1024))
}

/// Second whitespace-separated field of `/proc/self/statm`: the resident page
/// count.
#[cfg(any(target_os = "linux", test))]
fn parse_statm_resident(statm: &str) -> std::io::Result<u64> {
    statm
        .split_ascii_whitespace()
        .nth(1)
        .and_then(|f| f.parse::<u64>().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "no resident field in /proc/self/statm",
            )
        })
}

/// `ps -o rss=` output: one number, in kibibytes, with whitespace around it.
#[cfg(any(not(target_os = "linux"), test))]
fn parse_ps_rss_kib(out: &str) -> std::io::Result<u64> {
    out.trim()
        .parse::<u64>()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Page size in bytes, asked of `getconf` once and then remembered. That keeps
/// a libc dependency out of the tree for a number that cannot change while the
/// process runs. Falls back to 4096, which is right on the x86-64 Debian image
/// the router ships in.
#[cfg(any(target_os = "linux", test))]
fn page_size() -> u64 {
    static PAGE_SIZE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *PAGE_SIZE.get_or_init(|| {
        std::process::Command::new("getconf")
            .arg("PAGESIZE")
            .output()
            .ok()
            .and_then(|o| {
                String::from_utf8_lossy(&o.stdout)
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .filter(|n| *n > 0)
            .unwrap_or(4096)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const GB: u64 = 1024 * 1024 * 1024;

    fn gate() -> Admission {
        Admission::new(3 * GB, Duration::from_secs(30))
    }

    #[test]
    fn under_the_high_water_admits_and_carries_the_sample() {
        assert_eq!(gate().decide(GB), Admit::Ok { rss: GB });
    }

    #[test]
    fn equal_to_the_high_water_still_admits() {
        // The design says "over memory_high_water, return 503". Exactly at the
        // mark is not over it.
        assert_eq!(gate().decide(3 * GB), Admit::Ok { rss: 3 * GB });
    }

    #[test]
    fn one_byte_over_rejects_with_all_three_numbers() {
        assert_eq!(
            gate().decide(3 * GB + 1),
            Admit::Reject {
                rss: 3 * GB + 1,
                high_water: 3 * GB,
                retry_after: Duration::from_secs(30),
            }
        );
    }

    #[test]
    fn zero_high_water_rejects_all_but_an_empty_process() {
        let g = Admission::new(0, Duration::from_secs(1));
        assert!(matches!(g.decide(0), Admit::Ok { .. }));
        assert!(matches!(g.decide(1), Admit::Reject { .. }));
    }

    #[test]
    fn high_water_is_reported_unchanged() {
        assert_eq!(gate().high_water(), 3 * GB);
    }

    #[test]
    fn statm_resident_is_the_second_field() {
        // size resident shared text lib data dt, as the kernel writes it.
        assert_eq!(
            parse_statm_resident("1234 567 89 1 0 100 0\n").unwrap(),
            567
        );
    }

    #[test]
    fn statm_rejects_rubbish() {
        assert!(parse_statm_resident("").is_err());
        assert!(parse_statm_resident("1234\n").is_err());
        assert!(parse_statm_resident("1234 notanumber 89\n").is_err());
    }

    #[test]
    fn ps_rss_tolerates_the_padding_ps_adds() {
        assert_eq!(parse_ps_rss_kib("  41296  \n").unwrap(), 41296);
        assert!(parse_ps_rss_kib("  \n").is_err());
    }

    #[test]
    fn page_size_is_a_sane_power_of_two() {
        let p = page_size();
        assert!(p >= 4096 && p.is_power_of_two(), "page size was {p}");
    }

    #[test]
    fn the_real_read_returns_something_plausible() {
        let rss = read_rss_bytes().expect("this process has an RSS");
        assert!(rss > 1024 * 1024, "RSS looked impossibly small: {rss}");
    }
}
