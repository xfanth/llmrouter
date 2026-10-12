//! `GET /v1/models`: the merged catalogue, ranked.
//!
//! One row per distinct model id, not per channel. Gemini with 9 keys serving
//! the same model is one row whose `channels.total` is 9.
//!
//! Sorted by intelligence, highest first, with `auto` at the top. Every model
//! the router knows a channel for appears, healthy or not, because a client
//! filling a picker wants the whole list and the health counts tell it which
//! entries are worth choosing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::catalogue::Channel;

/// The id of the default row, which lets the router choose.
const AUTO: &str = "auto";
/// `owned_by` for the `auto` row: no single provider owns it.
const ROUTER: &str = "router";
/// The input modality that marks a model as able to read images.
const IMAGE: &str = "image";

/// The whole response.
#[derive(Debug, Serialize)]
pub struct ModelsResponse {
    /// Always `list`.
    pub object: String,
    /// One row per distinct model id, `auto` first.
    pub data: Vec<ModelRow>,
}

/// One model. The first four fields are the standard OpenAI ones, so an
/// unmodified client reads the list without changes. The rest are extras,
/// flat, in the order DESIGN.md prints them.
#[derive(Debug, Serialize)]
pub struct ModelRow {
    /// The model id a caller passes back in a request.
    pub id: String,
    /// Always `model`.
    pub object: String,
    /// Which provider serves it, or `router` for the `auto` row.
    pub owned_by: String,
    /// Unix seconds: the provider's own value for this model where it
    /// published one, else the router's boot time.
    pub created: u64,

    /// The general intelligence score. Higher is better. 0 when unranked.
    pub intelligence: u16,
    /// The best rank across this model's channels. Lower is better.
    pub intelligence_rank: u32,
    /// The largest context window across this model's channels.
    pub context_length: u32,
    /// The largest completion length across this model's channels.
    pub max_output: u32,
    /// Tool calling. Claude Code sends tools on every request, so a model
    /// without this is a guaranteed failure.
    pub supports_tools: bool,
    /// Takes images as input.
    pub supports_vision: bool,
    /// A reasoning model.
    pub supports_reasoning: bool,
    /// Live health across every channel serving this model.
    pub channels: ChannelSummary,
}

/// Live health for one model, counted across its channels.
#[derive(Debug, Serialize)]
pub struct ChannelSummary {
    /// Channels serving this model.
    pub total: u32,
    /// Channels callable right now.
    pub healthy: u32,
    /// Channels waiting out a backoff.
    pub in_backoff: u32,
    /// Which providers serve it, sorted and deduplicated.
    pub providers: Vec<String>,
    /// RFC 3339 instant at which the first in-backoff channel becomes
    /// eligible, for example `2026-10-11T03:14:22Z`. `None` when at least one
    /// channel is callable now.
    pub next_eligible: Option<String>,
}

/// Groups the channel list by model id and builds one row each.
///
/// The `auto` row goes first and carries the whole catalogue's totals: it is
/// the default, and it is what a client should pick unless it has a reason not
/// to.
/// `created` is the fallback used for a model whose providers published no
/// release date of their own, and for the `auto` row, which has none.
pub fn build_rows(channels: &[Channel], now_unix: u64, created: u64) -> Vec<ModelRow> {
    let mut by_model: BTreeMap<&str, Vec<&Channel>> = BTreeMap::new();
    for channel in channels {
        by_model.entry(&channel.model_id).or_default().push(channel);
    }

    let mut rows: Vec<ModelRow> = by_model
        .into_iter()
        .map(|(model_id, group)| {
            let owner = owner_of(&group, now_unix);
            // The newest date any provider gives for this model id, because
            // several providers serve it and they do not always agree.
            let created = group
                .iter()
                .filter_map(|c| c.created)
                .max()
                .unwrap_or(created);
            row(model_id.to_owned(), owner, &group, now_unix, created)
        })
        .collect();

    // Highest intelligence first. Ties break on id, so the list reads the same
    // way on every call rather than following the catalogue's shuffle.
    rows.sort_by(|a, b| {
        b.intelligence
            .cmp(&a.intelligence)
            .then_with(|| a.id.cmp(&b.id))
    });

    let all: Vec<&Channel> = channels.iter().collect();
    let mut out = Vec::with_capacity(rows.len() + 1);
    out.push(row(
        AUTO.to_owned(),
        ROUTER.to_owned(),
        &all,
        now_unix,
        created,
    ));
    out.append(&mut rows);
    out
}

/// Builds one row from the channels serving it. Used for a real model and for
/// the `auto` row, which is handed the whole catalogue. `created` is already
/// resolved by the caller.
fn row(id: String, owned_by: String, group: &[&Channel], now_unix: u64, created: u64) -> ModelRow {
    ModelRow {
        id,
        object: "model".to_owned(),
        owned_by,
        created,
        intelligence: group.iter().map(|c| c.intelligence).max().unwrap_or(0),
        intelligence_rank: group
            .iter()
            .map(|c| c.intelligence_rank)
            .min()
            .unwrap_or(u32::MAX),
        context_length: group.iter().map(|c| c.limit_context).max().unwrap_or(0),
        max_output: group.iter().map(|c| c.limit_output).max().unwrap_or(0),
        supports_tools: group.iter().any(|c| c.tool_call),
        supports_vision: group.iter().any(|c| {
            c.modalities_input
                .iter()
                .any(|m| m.eq_ignore_ascii_case(IMAGE))
        }),
        supports_reasoning: group.iter().any(|c| c.reasoning),
        channels: summarise(group, now_unix),
    }
}

/// The provider named in `owned_by`: the one serving the highest-ranked
/// healthy channel for this model.
///
/// Several providers can serve the same model id at different ranks, and the
/// full set stays in `channels.providers`. When nothing is healthy the
/// best-ranked channel still names the owner, because a row with no healthy
/// channel is the one a reader most wants attributed. Ties break on provider
/// name so the answer is the same on every call.
fn owner_of(group: &[&Channel], now_unix: u64) -> String {
    let best = |healthy_only: bool| {
        group
            .iter()
            .filter(|c| !healthy_only || c.eligible(now_unix))
            .min_by(|a, b| {
                a.intelligence_rank
                    .cmp(&b.intelligence_rank)
                    .then_with(|| a.provider.cmp(&b.provider))
            })
            .map(|c| c.provider.to_string())
    };
    best(true)
        .or_else(|| best(false))
        .unwrap_or_else(|| ROUTER.to_owned())
}

/// Counts live health across the channels serving one model.
///
/// A channel is healthy when it is enabled and its backoff deadline has
/// passed, and in backoff when that deadline is still ahead. A disabled
/// channel is neither: it counts in `total` and nowhere else, because waiting
/// does not bring it back.
fn summarise(group: &[&Channel], now_unix: u64) -> ChannelSummary {
    let mut healthy = 0u32;
    let mut in_backoff = 0u32;
    let mut next_eligible: Option<u64> = None;

    for channel in group {
        let enabled = channel.health.enabled.load(Ordering::Relaxed);
        let eligible_at = channel.health.eligible_at.load(Ordering::Relaxed);
        if eligible_at > now_unix {
            in_backoff += 1;
            next_eligible = Some(next_eligible.map_or(eligible_at, |n: u64| n.min(eligible_at)));
        } else if enabled {
            healthy += 1;
        }
    }

    ChannelSummary {
        total: group.len() as u32,
        healthy,
        in_backoff,
        providers: providers_of(group),
        // The wait only matters when nothing is callable now.
        next_eligible: if healthy > 0 {
            None
        } else {
            next_eligible.map(rfc3339)
        },
    }
}

/// The last second `SystemTime` can hold on every platform this builds for,
/// which is `9999-12-31T23:59:59Z`.
const LAST_FORMATTABLE_SECOND: u64 = 253_402_300_799;

/// Unix seconds as `2026-10-11T03:14:22Z`. `humantime` does the formatting, so
/// there is no date arithmetic here.
///
/// `eligible_at` is a plain `u64` that the backoff arithmetic saturates, so a
/// provider sending an absurd `Retry-After` date can push it past what
/// `SystemTime` can represent. Adding that to the epoch panics, and this runs
/// inside a `GET` handler, so the value is clamped instead: a deadline that far
/// out is not one any caller can wait for either way.
fn rfc3339(unix_secs: u64) -> String {
    let secs = unix_secs.min(LAST_FORMATTABLE_SECOND);
    humantime::format_rfc3339_seconds(UNIX_EPOCH + Duration::from_secs(secs)).to_string()
}

/// The distinct providers serving these channels, sorted.
fn providers_of(group: &[&Channel]) -> Vec<String> {
    let set: BTreeSet<&str> = group.iter().map(|c| &*c.provider).collect();
    set.into_iter().map(str::to_owned).collect()
}

/// `GET /v1/models`.
pub async fn get_models(state: axum::extract::State<Arc<crate::server::AppState>>) -> Response {
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    // Boot time is only the fallback: a row whose provider published its own
    // `created` reports that instead.
    let created = now_unix.saturating_sub(state.started_at.elapsed().as_secs());

    let snapshot = state.catalogue.snapshot();
    axum::Json(ModelsResponse {
        object: "list".to_owned(),
        data: build_rows(&snapshot, now_unix, created),
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogue::{ChannelHealth, ChannelId, ChannelTelemetry};
    use crate::config::WireName;

    /// Builds a channel with everything the rows read and sensible blanks for
    /// the rest. `eligible_at` of 0 means callable now.
    #[allow(clippy::too_many_arguments)]
    fn channel(
        provider: &str,
        model_id: &str,
        key_id: &str,
        intelligence: u16,
        limit_context: u32,
        enabled: bool,
        eligible_at: u64,
    ) -> Channel {
        Channel {
            id: ChannelId(0),
            provider: provider.into(),
            model_id: model_id.into(),
            key_id: key_id.into(),
            key: "unused".into(),
            base_url: "https://example.invalid".into(),
            wire: WireName::OpenAi,
            limit_context,
            limit_output: 4096,
            tool_call: true,
            reasoning: false,
            structured_output: false,
            modalities_input: vec!["text".to_owned()],
            intelligence,
            intelligence_rank: 0,
            created: None,
            health: ChannelHealth {
                enabled: enabled.into(),
                eligible_at: eligible_at.into(),
                ..ChannelHealth::default()
            },
            telemetry: ChannelTelemetry::default(),
        }
    }

    #[test]
    fn empty_catalogue_still_offers_auto() {
        let rows = build_rows(&[], 1000, 500);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, AUTO);
        assert_eq!(rows[0].owned_by, ROUTER);
        assert_eq!(rows[0].channels.total, 0);
        assert_eq!(rows[0].channels.healthy, 0);
        assert_eq!(rows[0].channels.next_eligible, None);
        assert_eq!(rows[0].created, 500);
    }

    #[test]
    fn a_model_reports_the_providers_release_date_not_boot_time() {
        let mut dated = channel("openrouter", "claude-fable-5-1", "k0", 71, 200_000, true, 0);
        dated.created = Some(1_760_140_800);
        let older = {
            let mut c = channel("zai", "claude-fable-5-1", "k1", 71, 200_000, true, 0);
            c.created = Some(1_700_000_000);
            c
        };
        let undated = channel("gemini", "gemini-3.7-flash", "k2", 50, 1_000_000, true, 0);

        let rows = build_rows(&[dated, older, undated], 1000, 500);
        let claude = rows
            .iter()
            .find(|r| r.id == "claude-fable-5-1")
            .expect("row");
        assert_eq!(claude.created, 1_760_140_800, "newest provider date wins");
        let gemini = rows
            .iter()
            .find(|r| r.id == "gemini-3.7-flash")
            .expect("row");
        assert_eq!(
            gemini.created, 500,
            "no provider date falls back to boot time"
        );
        assert_eq!(rows[0].created, 500, "the auto row has no release date");
    }

    #[test]
    fn nine_keys_on_one_model_are_one_row() {
        let channels: Vec<Channel> = (0..9)
            .map(|n| {
                channel(
                    "gemini",
                    "gemini-3.7-flash",
                    &format!("key{n}"),
                    50,
                    1_000_000,
                    true,
                    0,
                )
            })
            .collect();

        let rows = build_rows(&channels, 1000, 0);
        assert_eq!(rows.len(), 2, "auto plus one model row");
        let model = &rows[1];
        assert_eq!(model.id, "gemini-3.7-flash");
        assert_eq!(model.owned_by, "gemini");
        assert_eq!(model.channels.total, 9);
        assert_eq!(model.channels.healthy, 9);
        assert_eq!(model.channels.providers, vec!["gemini".to_owned()]);
    }

    #[test]
    fn auto_leads_and_the_rest_sort_by_intelligence() {
        let channels = vec![
            channel("a", "mid", "k", 50, 100, true, 0),
            channel("b", "best", "k", 90, 100, true, 0),
            channel("c", "worst", "k", 10, 100, true, 0),
        ];

        let ids: Vec<String> = build_rows(&channels, 1000, 0)
            .into_iter()
            .map(|r| r.id)
            .collect();
        assert_eq!(ids, vec!["auto", "best", "mid", "worst"]);
    }

    #[test]
    fn equal_intelligence_breaks_on_id() {
        let channels = vec![
            channel("a", "zeta", "k", 50, 100, true, 0),
            channel("b", "alpha", "k", 50, 100, true, 0),
        ];

        let ids: Vec<String> = build_rows(&channels, 1000, 0)
            .into_iter()
            .map(|r| r.id)
            .collect();
        assert_eq!(ids, vec!["auto", "alpha", "zeta"]);
    }

    #[test]
    fn unhealthy_models_still_appear() {
        let channels = vec![channel("a", "dead", "k", 50, 100, true, 2000)];
        let rows = build_rows(&channels, 1000, 0);

        assert_eq!(rows[1].id, "dead");
        assert_eq!(rows[1].channels.total, 1);
        assert_eq!(rows[1].channels.healthy, 0);
        assert_eq!(rows[1].channels.in_backoff, 1);
        assert_eq!(
            rows[1].channels.next_eligible.as_deref(),
            Some("1970-01-01T00:33:20Z")
        );
    }

    #[test]
    fn next_eligible_is_the_soonest_and_only_when_nothing_is_callable() {
        let all_waiting = vec![
            channel("a", "m", "k1", 50, 100, true, 9000),
            channel("a", "m", "k2", 50, 100, true, 3000),
        ];
        let rows = build_rows(&all_waiting, 1000, 0);
        assert_eq!(
            rows[1].channels.next_eligible.as_deref(),
            Some("1970-01-01T00:50:00Z"),
            "3000 seconds past the epoch, not 9000"
        );

        let one_live = vec![
            channel("a", "m", "k1", 50, 100, true, 9000),
            channel("a", "m", "k2", 50, 100, true, 0),
        ];
        let rows = build_rows(&one_live, 1000, 0);
        assert_eq!(rows[1].channels.healthy, 1);
        assert_eq!(rows[1].channels.in_backoff, 1);
        assert_eq!(rows[1].channels.next_eligible, None);
    }

    #[test]
    fn next_eligible_prints_the_shape_the_spec_shows() {
        // The worked example in DESIGN.md.
        assert_eq!(rfc3339(1_791_688_462), "2026-10-11T03:14:22Z");
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn a_deadline_already_past_means_callable_now() {
        let channels = vec![channel("a", "m", "k", 50, 100, true, 999)];
        let rows = build_rows(&channels, 1000, 0);
        assert_eq!(rows[1].channels.healthy, 1);
        assert_eq!(rows[1].channels.in_backoff, 0);
    }

    #[test]
    fn a_disabled_channel_is_neither_healthy_nor_in_backoff() {
        let channels = vec![channel("a", "m", "k", 50, 100, false, 0)];
        let rows = build_rows(&channels, 1000, 0);

        assert_eq!(rows[1].channels.total, 1);
        assert_eq!(rows[1].channels.healthy, 0);
        assert_eq!(rows[1].channels.in_backoff, 0);
        assert_eq!(rows[1].channels.next_eligible, None);
    }

    #[test]
    fn capabilities_and_limits_merge_across_channels() {
        let mut big = channel("openrouter", "m", "k1", 50, 200_000, true, 0);
        big.reasoning = true;
        big.limit_output = 64_000;
        big.intelligence_rank = 3;
        big.modalities_input = vec!["image".to_owned(), "text".to_owned()];
        let mut small = channel("groq", "m", "k2", 50, 8_192, true, 0);
        small.tool_call = false;
        small.intelligence_rank = 11;

        let rows = build_rows(&[big, small], 1000, 0);
        let model = &rows[1];

        assert_eq!(model.context_length, 200_000);
        assert_eq!(model.max_output, 64_000, "the larger of 64000 and 4096");
        assert_eq!(model.intelligence_rank, 3, "the better of rank 3 and 11");
        assert!(model.supports_tools, "one channel takes tools");
        assert!(model.supports_reasoning);
        assert!(model.supports_vision, "one channel takes image input");
        assert_eq!(
            model.channels.providers,
            vec!["groq".to_owned(), "openrouter".to_owned()]
        );
        assert_eq!(
            model.owned_by, "openrouter",
            "the highest-ranked healthy channel, not the first alphabetically"
        );
    }

    #[test]
    fn a_text_only_model_does_not_support_vision() {
        let rows = build_rows(&[channel("a", "m", "k", 50, 100, true, 0)], 1000, 0);
        assert!(!rows[1].supports_vision);
        assert!(!rows[1].supports_reasoning);
    }

    #[test]
    fn owned_by_skips_a_better_ranked_channel_that_is_in_backoff() {
        let mut backed_off = channel("openrouter", "m", "k1", 50, 100, true, 5000);
        backed_off.intelligence_rank = 1;
        let mut live = channel("zai", "m", "k2", 50, 100, true, 0);
        live.intelligence_rank = 9;

        let rows = build_rows(&[backed_off, live], 1000, 0);
        assert_eq!(rows[1].owned_by, "zai");
    }

    #[test]
    fn owned_by_falls_back_to_the_best_rank_when_nothing_is_healthy() {
        let mut worse = channel("groq", "m", "k1", 50, 100, true, 5000);
        worse.intelligence_rank = 40;
        let mut better = channel("openrouter", "m", "k2", 50, 100, true, 5000);
        better.intelligence_rank = 2;

        let rows = build_rows(&[worse, better], 1000, 0);
        assert_eq!(rows[1].channels.healthy, 0);
        assert_eq!(rows[1].owned_by, "openrouter");
    }

    #[test]
    fn owned_by_breaks_a_rank_tie_on_provider_name() {
        let channels = vec![
            channel("zai", "m", "k1", 50, 100, true, 0),
            channel("groq", "m", "k2", 50, 100, true, 0),
        ];
        let rows = build_rows(&channels, 1000, 0);
        assert_eq!(rows[1].owned_by, "groq");
    }

    #[test]
    fn auto_carries_the_whole_catalogue() {
        let channels = vec![
            channel("a", "one", "k", 90, 1000, true, 0),
            channel("b", "two", "k", 10, 500, true, 4000),
        ];
        let auto = &build_rows(&channels, 1000, 0)[0];

        assert_eq!(auto.channels.total, 2);
        assert_eq!(auto.channels.healthy, 1);
        assert_eq!(auto.channels.in_backoff, 1);
        assert_eq!(auto.intelligence, 90);
        assert_eq!(auto.context_length, 1000);
        assert_eq!(auto.owned_by, ROUTER, "no single provider owns auto");
        assert_eq!(
            auto.channels.providers,
            vec!["a".to_owned(), "b".to_owned()]
        );
    }

    #[test]
    fn the_row_serialises_the_fields_the_spec_lists_in_order() {
        let mut vision = channel("a", "m", "k", 50, 200_000, true, 0);
        vision.modalities_input = vec!["text".to_owned(), "image".to_owned()];
        vision.limit_output = 64_000;
        vision.intelligence_rank = 3;

        let response = ModelsResponse {
            object: "list".to_owned(),
            data: build_rows(&[vision], 1000, 777),
        };
        let json = serde_json::to_value(&response).expect("serialises");
        let row = &json["data"][1];

        assert_eq!(json["object"], "list");
        assert_eq!(row["id"], "m");
        assert_eq!(row["object"], "model");
        assert_eq!(row["created"], 777);
        assert_eq!(row["owned_by"], "a");
        assert_eq!(row["intelligence"], 50);
        assert_eq!(row["intelligence_rank"], 3);
        assert_eq!(row["context_length"], 200_000);
        assert_eq!(row["max_output"], 64_000);
        assert_eq!(row["supports_tools"], true);
        assert_eq!(row["supports_vision"], true);
        assert_eq!(row["supports_reasoning"], false);
        assert!(row["channels"]["next_eligible"].is_null());
        assert!(
            row.get("capabilities").is_none(),
            "the flags are flat now, not nested"
        );

        // The standard OpenAI fields come first, then the extras. Checked on
        // the wire text, because `serde_json::Value` sorts its keys.
        let text = serde_json::to_string(&response).expect("serialises");
        let order = [
            "\"id\"",
            "\"object\"",
            "\"owned_by\"",
            "\"created\"",
            "\"intelligence\"",
            "\"intelligence_rank\"",
            "\"context_length\"",
            "\"max_output\"",
            "\"supports_tools\"",
            "\"supports_vision\"",
            "\"supports_reasoning\"",
            "\"channels\"",
        ];
        // Skip the `auto` row and read the field order off the model row.
        let tail = &text[text.rfind("\"id\"").expect("a row")..];
        let mut at = 0usize;
        for field in order {
            let found = tail[at..]
                .find(field)
                .unwrap_or_else(|| panic!("{field} is missing or out of order"));
            at += found + field.len();
        }
    }
}
