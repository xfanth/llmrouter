//! llmrouter: races many provider/model/key channels and returns the first
//! good answer.
//!
//! This file declares the modules and re-exports the handful of types that
//! cross module boundaries. Ownership of each type stays with the module that
//! defines it; see ARCHITECTURE.md for the table.

pub mod admission;
pub mod cache;
pub mod catalogue;
pub mod config;
pub mod dashboard;
pub mod health;
pub mod history;
pub mod logging;
pub mod metrics;
pub mod models_api;
pub mod net;
pub mod probe;
pub mod race;
pub mod reqlog;
pub mod server;
pub mod upstream;
pub mod wire;

pub use admission::{Admission, Admit};
pub use cache::{Cache, CacheEntry, CacheKey};
pub use catalogue::{Catalogue, Channel, ChannelFilter, ChannelHealth, ChannelId};
pub use config::{
    CacheConfig, Capability, Config, HealthPollConfig, IntelligenceEntry, ModelOverride, Provider,
    RouterConfig, WireName,
};
pub use health::{HealthEntry, HealthKey, HealthTable};
pub use logging::{LogSetup, StartupSummary};
pub use metrics::{AttemptOutcome, Metrics, Outcome};
pub use net::{LinkState, NetTest};
pub use probe::{ProbeOutcome, ProbedModel, Prober};
pub use race::{RaceInput, RaceOutcome, Racer, Winner};
pub use server::{ApiError, AppState};
pub use upstream::{CallError, CallOutcome, StreamedCall, Upstream};
pub use wire::{Format, NeutralRequest, NeutralResponse, WireError};

/// Anything that can end a client request before it reaches a channel.
#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    /// A config file is missing, unreadable or malformed.
    #[error("config: {0}")]
    Config(String),

    /// A redb store could not be opened, read or written.
    #[error("store: {0}")]
    Store(String),

    /// Wire-format translation failed at one of the two edges.
    #[error("wire: {0}")]
    Wire(#[from] WireError),

    /// An outbound channel call could not be made.
    #[error("upstream: {0}")]
    Upstream(#[from] CallError),
}

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, RouterError>;
