//! The network test.
//!
//! Two calls to two different operators, 2 second timeout, running beside every
//! client request's channel calls rather than after a failure. The answer then
//! arrives at the same time as the failures instead of after them.
//!
//! Both failing means the local link is down. That is not any provider's
//! fault, so no `retry_counter` is touched, the walk is cancelled, and the
//! client gets `502`.

use std::sync::Arc;
use std::time::Duration;

use futures::stream::{FuturesUnordered, Stream, StreamExt};

/// Google's 204 endpoint.
pub const TEST_URL_GOOGLE: &str = "https://www.google.com/generate_204";
/// Cloudflare's trace endpoint.
pub const TEST_URL_CLOUDFLARE: &str = "https://cloudflare.com/cdn-cgi/trace";

/// What the two test calls found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkState {
    /// At least one test call answered. Channel failures are the providers'
    /// fault and count against their backoff as normal.
    Up,
    /// Both test calls failed. The fault is local.
    Down {
        /// Which URLs failed and why, for the `502` body. No key material.
        failed: Vec<String>,
    },
}

/// One test call's result: `Ok` is an answer of any kind, `Err` carries the
/// line that goes in the `502` body.
type Outcome = Result<(), String>;

/// Runs the two test calls over the shared client.
#[derive(Debug, Clone)]
pub struct NetTest {
    client: reqwest::Client,
    timeout: Duration,
    /// Where the two calls go. The operator endpoints above in production.
    /// They are a field rather than two constants read inline so the race can
    /// be exercised against a local server: the link being up or down decides
    /// whether a drained walk is a `502` or a `504`, and that rule is only
    /// testable if the test can say which.
    urls: Arc<Vec<String>>,
}

impl NetTest {
    /// Shares the router's one client, so the test uses the same pool as the
    /// channel calls and tests the same path.
    pub fn new(client: reqwest::Client, timeout: Duration) -> Self {
        Self::with_urls(
            client,
            timeout,
            vec![TEST_URL_GOOGLE.to_string(), TEST_URL_CLOUDFLARE.to_string()],
        )
    }

    /// The same test against URLs of the caller's choosing.
    pub fn with_urls(client: reqwest::Client, timeout: Duration, urls: Vec<String>) -> Self {
        Self {
            client,
            timeout,
            urls: Arc::new(urls),
        }
    }

    /// Fires both calls at once and returns as soon as either answers. Any
    /// HTTP status counts as an answer; only a transport failure or the
    /// timeout counts as a fail.
    pub async fn check(&self) -> LinkState {
        let calls: FuturesUnordered<_> =
            self.urls.iter().map(|url| self.one(url.as_str())).collect();
        settle(calls).await
    }

    /// One test call. A reply of any status means the link carried it, so the
    /// status is never read.
    async fn one(&self, url: &str) -> Outcome {
        match self.client.get(url).timeout(self.timeout).send().await {
            Ok(_) => Ok(()),
            Err(err) => Err(describe_failure(url, &err)),
        }
    }
}

/// Returns `Up` on the first answer without waiting for the rest, and `Down`
/// carrying every reason only once they have all failed.
async fn settle<S>(mut outcomes: S) -> LinkState
where
    S: Stream<Item = Outcome> + Unpin,
{
    let mut failed = Vec::new();
    while let Some(outcome) = outcomes.next().await {
        match outcome {
            Ok(()) => return LinkState::Up,
            Err(reason) => failed.push(reason),
        }
    }
    LinkState::Down { failed }
}

/// The `502` body line for one failed call. The URLs are fixed and public and
/// the test carries no key, so nothing here can leak key material.
fn describe_failure(url: &str, err: &reqwest::Error) -> String {
    let kind = if err.is_timeout() {
        "timed out"
    } else if err.is_connect() {
        "connect failed"
    } else {
        "request failed"
    };
    format!("{url}: {kind}: {err}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    fn fail(reason: &str) -> Outcome {
        Err(reason.to_string())
    }

    #[test]
    fn both_operators_are_different() {
        assert_ne!(TEST_URL_GOOGLE, TEST_URL_CLOUDFLARE);
    }

    #[test]
    fn first_answer_wins() {
        let outcomes = stream::iter(vec![Ok(()), fail("not reached")]);
        assert_eq!(tokio_test::block_on(settle(outcomes)), LinkState::Up);
    }

    #[test]
    fn one_failure_then_an_answer_is_still_up() {
        let outcomes = stream::iter(vec![fail("google: timed out"), Ok(())]);
        assert_eq!(tokio_test::block_on(settle(outcomes)), LinkState::Up);
    }

    #[test]
    fn both_failing_is_down_and_keeps_every_reason() {
        let outcomes = stream::iter(vec![fail("google: timed out"), fail("cloudflare: connect")]);
        assert_eq!(
            tokio_test::block_on(settle(outcomes)),
            LinkState::Down {
                failed: vec![
                    "google: timed out".to_string(),
                    "cloudflare: connect".to_string(),
                ],
            }
        );
    }

    #[test]
    fn no_calls_at_all_is_down_with_nothing_to_report() {
        let outcomes = stream::iter(Vec::<Outcome>::new());
        assert_eq!(
            tokio_test::block_on(settle(outcomes)),
            LinkState::Down { failed: Vec::new() }
        );
    }

    #[test]
    fn a_failure_line_names_the_url() {
        // Built from a real error, so the wording tracks the real client. A
        // bad scheme fails without touching the network.
        let client = reqwest::Client::new();
        let err = tokio_test::block_on(async {
            client
                .get("notascheme://nowhere.invalid/")
                .send()
                .await
                .unwrap_err()
        });
        let line = describe_failure(TEST_URL_GOOGLE, &err);
        assert!(line.starts_with(TEST_URL_GOOGLE));
        assert!(line.contains("failed") || line.contains("timed out"));
    }
}
