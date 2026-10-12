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
use std::time::{SystemTime, UNIX_EPOCH};

use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::catalogue::Channel;

/// The id of the default row, which lets the router choose.
const AUTO: &str = "auto";
/// `owned_by` for the `auto` row: no single provider owns it.
const ROUTER: &str = "router";

/// The whole response.
#[derive(Debug, Serialize)]
pub struct ModelsResponse {
    /// Always `list`.
    pub object: String,
    /// One row per distinct model id, `auto` first.
    pub data: Vec<ModelRow>,
}

/// One model. The first four fields are the standard OpenAI ones, so an
/// unmodified client reads the list without changes. The rest are extras.
#[derive(Debug, Serialize)]
pub struct ModelRow {
    /// The model id a caller passes back in a request.
    pub id: String,
    /// Always `model`.
    pub object: String,
    /// Unix seconds. The router has no creation date, so this is boot time.
    pub created: u64,
    /// Which provider serves it, or `router` for the `auto` row.
    pub owned_by: String,

    /// The general intelligence score. Higher is better. 0 when unranked.
    pub intelligence: u16,
    /// The largest context window across this model's channels.
    pub context_length: u32,
    /// What the model can do.
    pub capabilities: Capabilities,
    /// Live health across every channel serving this model.
    pub channels: ChannelSummary,
}

/// The capability flags, merged across the model's channels.
#[derive(Debug, Serialize)]
pub struct Capabilities {
    /// Tool calling. Claude Code sends tools on every request, so a model
    /// without this is a guaranteed failure.
    pub tool_call: bool,
    /// Reasoning model.
    pub reasoning: bool,
    /// Structured output.
    pub structured_output: bool,
    /// Input modalities, for example `text`, `image`.
    pub modalities_input: Vec<String>,
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
    /// Unix seconds at which the first in-backoff channel becomes eligible.
    /// `None` when at least one channel is callable now.
    pub next_eligible: Option<u64>,
}

/// Groups the channel list by model id and builds one row each.
///
/// The `auto` row goes first and carries the whole catalogue's totals: it is
/// the default, and it is what a client should pick unless it has a reason not
/// to.
pub fn build_rows(channels: &[Channel], now_unix: u64, created: u64) -> Vec<ModelRow> {
    let mut by_model: BTreeMap<&str, Vec<&Channel>> = BTreeMap::new();
    for channel in channels {
        by_model.entry(&channel.model_id).or_default().push(channel);
    }

    let mut rows: Vec<ModelRow> = by_model
        .into_iter()
        .map(|(model_id, group)| {
            // Several providers can serve the same model id, so the row is
            // owned by the first alphabetically and the full set stays in
            // `channels.providers`.
            let owned_by = providers_of(&group)
                .into_iter()
                .next()
                .unwrap_or_else(|| ROUTER.to_owned());
            row(model_id.to_owned(), owned_by, &group, now_unix, created)
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
/// the `auto` row, which is handed the whole catalogue.
fn row(id: String, owned_by: String, group: &[&Channel], now_unix: u64, created: u64) -> ModelRow {
    let mut modalities: BTreeSet<&str> = BTreeSet::new();
    for channel in group {
        modalities.extend(channel.modalities_input.iter().map(String::as_str));
    }

    ModelRow {
        id,
        object: "model".to_owned(),
        created,
        owned_by,
        intelligence: group.iter().map(|c| c.intelligence).max().unwrap_or(0),
        context_length: group.iter().map(|c| c.limit_context).max().unwrap_or(0),
        capabilities: Capabilities {
            tool_call: group.iter().any(|c| c.tool_call),
            reasoning: group.iter().any(|c| c.reasoning),
            structured_output: group.iter().any(|c| c.structured_output),
            modalities_input: modalities.into_iter().map(str::to_owned).collect(),
        },
        channels: summarise(group, now_unix),
    }
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
        next_eligible: if healthy > 0 { None } else { next_eligible },
    }
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
    // The router has no creation date, so every row reports boot time.
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
        assert_eq!(rows[1].channels.next_eligible, Some(2000));
    }

    #[test]
    fn next_eligible_is_the_soonest_and_only_when_nothing_is_callable() {
        let all_waiting = vec![
            channel("a", "m", "k1", 50, 100, true, 9000),
            channel("a", "m", "k2", 50, 100, true, 3000),
        ];
        let rows = build_rows(&all_waiting, 1000, 0);
        assert_eq!(rows[1].channels.next_eligible, Some(3000));

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
    fn capabilities_and_context_merge_across_channels() {
        let mut big = channel("openrouter", "m", "k1", 50, 200_000, true, 0);
        big.reasoning = true;
        big.modalities_input = vec!["image".to_owned(), "text".to_owned()];
        let mut small = channel("groq", "m", "k2", 50, 8_192, true, 0);
        small.tool_call = false;
        small.structured_output = true;

        let rows = build_rows(&[big, small], 1000, 0);
        let model = &rows[1];

        assert_eq!(model.context_length, 200_000);
        assert!(model.capabilities.tool_call);
        assert!(model.capabilities.reasoning);
        assert!(model.capabilities.structured_output);
        assert_eq!(
            model.capabilities.modalities_input,
            vec!["image".to_owned(), "text".to_owned()]
        );
        assert_eq!(
            model.channels.providers,
            vec!["groq".to_owned(), "openrouter".to_owned()]
        );
        assert_eq!(model.owned_by, "groq", "first provider alphabetically");
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
        assert_eq!(
            auto.channels.providers,
            vec!["a".to_owned(), "b".to_owned()]
        );
    }

    #[test]
    fn rows_carry_the_standard_openai_fields() {
        let channels = vec![channel("a", "m", "k", 50, 100, true, 0)];
        let response = ModelsResponse {
            object: "list".to_owned(),
            data: build_rows(&channels, 1000, 777),
        };
        let json = serde_json::to_value(&response).expect("serialises");

        assert_eq!(json["object"], "list");
        assert_eq!(json["data"][1]["id"], "m");
        assert_eq!(json["data"][1]["object"], "model");
        assert_eq!(json["data"][1]["created"], 777);
        assert_eq!(json["data"][1]["owned_by"], "a");
        assert!(json["data"][1]["channels"]["next_eligible"].is_null());
    }
}
