//! The two-wave boot sequence, end to end.
//!
//! `Prober::run_startup_waves` is the only thing that decides what the
//! catalogue looks like at boot, and its unit-tested parts say nothing about
//! the orchestration between them. This drives the real thing against wiremock
//! with two providers and two keys: one provider alive with one good key and
//! one rejected key, one provider whose host does not answer at all.
//!
//! The load-bearing assertion is that the dead provider's key pairs have no
//! health row at all. A wave 2 built from the configured provider list rather
//! than the reachable one would probe them and leave failure rows behind, and
//! nothing else about the boot would look wrong.
//!
//! Keys are obvious fakes.

use std::collections::HashMap;
use std::sync::Arc;

use llmrouter::probe::key_id;
use llmrouter::{
    Catalogue, Config, HealthKey, HealthTable, Prober, Provider, RouterConfig, Upstream, WireName,
};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const GOOD_KEY: &str = "sk-fake-good";
const BAD_KEY: &str = "sk-fake-expired";

/// A provider that answers `/models`: `401` without a key, the model list with
/// the good key, `401` with the expired one.
async fn live_provider() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/models"))
        .and(header("authorization", format!("Bearer {GOOD_KEY}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "object": "list",
            "data": [
                { "id": "fast-1", "context_length": 128000 },
                { "id": "fast-2", "context_length": 32000 }
            ]
        })))
        .mount(&server)
        .await;
    // Everything else, keyless probe included, is refused but reachable.
    Mock::given(method("GET"))
        .and(path("/models"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn startup_waves_build_the_catalogue_from_reachable_providers_only() {
    let live = live_provider().await;
    let dir = tempfile::tempdir().expect("a throwaway directory");

    let providers = vec![
        Provider {
            name: "live".to_string(),
            wire: WireName::OpenAi,
            base_url: live.uri(),
            keys_env: "LIVE_KEYS".to_string(),
            models_path: "/models".to_string(),
            models: Vec::new(),
        },
        Provider {
            name: "dead".to_string(),
            wire: WireName::OpenAi,
            // Nothing listens here, so the keyless probe is a transport
            // failure rather than an HTTP status.
            base_url: "http://127.0.0.1:1".to_string(),
            keys_env: "DEAD_KEYS".to_string(),
            models_path: "/models".to_string(),
            models: Vec::new(),
        },
    ];
    let keys = HashMap::from([
        (
            "LIVE_KEYS".to_string(),
            vec![GOOD_KEY.into(), BAD_KEY.into()],
        ),
        (
            "DEAD_KEYS".to_string(),
            vec![GOOD_KEY.into(), BAD_KEY.into()],
        ),
    ]);

    let router = RouterConfig::default();
    let config = Config {
        router: router.clone(),
        providers,
        models: Vec::new(),
        intelligence: Vec::new(),
        keys,
        capabilities: HashMap::new(),
    };
    let db = Arc::new(
        redb::Database::create(dir.path().join("state.redb")).expect("the scratch store opens"),
    );
    let health = Arc::new(
        HealthTable::open(
            db,
            router.health_poll.retry_base,
            router.health_poll.backoff_max,
        )
        .expect("the health table opens"),
    );
    let catalogue = Arc::new(Catalogue::new());
    let prober = Prober::new(
        Arc::new(config),
        Upstream::new(&router).expect("the client builds"),
        Arc::clone(&catalogue),
        Arc::clone(&health),
    );

    let summary = prober.run_startup_waves().await;

    // The expired key answered with `401`, so it reached the host but fetched
    // no models. Counting it as answered would report 2 of 2 keys working on a
    // run where half the keys are dead.
    assert_eq!(
        (summary.keys, summary.keys_answered),
        (2, 1),
        "only the good key fetched a model list"
    );

    // Wave 2 ran only the live provider's good key, so only its two models
    // are channels.
    let mut built: Vec<String> = catalogue
        .snapshot()
        .iter()
        .map(|c| format!("{}/{}/{}", c.provider, c.model_id, c.key_id))
        .collect();
    built.sort();
    let good = key_id("live", GOOD_KEY);
    assert_eq!(
        built,
        [format!("live/fast-1/{good}"), format!("live/fast-2/{good}")]
    );

    // Wave 1 put the unreachable provider in backoff.
    assert!(
        health
            .get(&HealthKey::Provider("dead".to_string()))
            .is_some_and(|e| e.retry_counter > 0),
        "a provider whose host never answered must be in backoff"
    );
    assert!(
        health
            .get(&HealthKey::Provider("live".to_string()))
            .is_none_or(|e| e.retry_counter == 0),
        "the live provider answered and has a working key, so it is healthy"
    );

    // The expired key on the live provider is down, on its own.
    assert!(
        health
            .get(&HealthKey::ProviderKey {
                provider: "live".to_string(),
                key_id: key_id("live", BAD_KEY),
            })
            .is_some_and(|e| e.retry_counter > 0),
        "the rejected key must be in backoff"
    );

    // And nothing spent a keyed call on the provider wave 1 already ruled out.
    for key in [GOOD_KEY, BAD_KEY] {
        assert!(
            health
                .get(&HealthKey::ProviderKey {
                    provider: "dead".to_string(),
                    key_id: key_id("dead", key),
                })
                .is_none(),
            "wave 2 must not probe keys on an unreachable provider"
        );
    }
}
