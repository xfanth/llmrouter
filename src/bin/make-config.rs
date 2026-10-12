//! `llmrouter-make-config`: turns the source data into the six files
//! `src/config.rs` reads.
//!
//! The source side is `providers.yaml` (which holds live keys inline),
//! `models.csv` and `model_capabilities.csv`. The output side is a config
//! directory holding `.env`, `providers.yml`, `models.yml`,
//! `intelligence.yml`, `router.yml` and `model_capabilities.csv`.
//!
//! The shape mismatch this bridges: `providers.yaml` carries its keys inline
//! plus `priority` and `known_models`; `providers.yml` carries a `keys_env`
//! naming a variable in `.env` and has no field for either of the other two.
//!
//! A key is never printed, logged or written anywhere but `.env`, which is
//! created mode 0600.

use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use clap::Parser;
use comfy_table::Table;
use llmrouter::config::{IntelligenceEntry, Provider, RouterConfig, WireName};
use serde::Deserialize;

/// Where the router reads its config in production. Outside the repo, and not
/// a git worktree, so nothing written there is ever committed.
const DEFAULT_OUT: &str = "/mnt/shuttle/share/app-data/llmrouter/config";

#[derive(Parser, Debug)]
#[command(
    name = "llmrouter-make-config",
    about = "Builds the router's config directory from providers.yaml, models.csv and model_capabilities.csv"
)]
struct Args {
    /// Directory holding `providers.yaml`, `models.csv` and
    /// `model_capabilities.csv`. Defaults to the working directory, which is
    /// where the data sits beside the repo.
    #[arg(long, default_value = ".")]
    source: PathBuf,

    /// Directory to write the six config files into. Created if missing.
    #[arg(long, default_value = DEFAULT_OUT)]
    out: PathBuf,

    /// Overwrite an existing `.env`. Without this the run refuses, because
    /// the file on disk may hold keys this source data no longer has.
    #[arg(long)]
    force: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let report = generate(&args.source, &args.out, args.force)?;
    print!("{}", report.render(&args.out));
    Ok(())
}

// ---------------------------------------------------------------------------
// Source shapes
// ---------------------------------------------------------------------------

/// `providers.yaml`, as `build_providers.py` writes it.
#[derive(Debug, Deserialize)]
struct SourceFile {
    providers: Vec<SourceProvider>,
}

/// One provider in `providers.yaml`. `priority` and `known_models` are read
/// and dropped: `providers.yml` has no field for either. Unknown keys are
/// ignored rather than fatal, since the file is generated upstream and may
/// grow columns.
#[derive(Debug, Deserialize)]
struct SourceProvider {
    name: String,
    wire: WireName,
    base_url: String,
    #[serde(default)]
    keys: Vec<String>,
    #[serde(default)]
    models_path: Option<String>,
}

/// One row of `models.csv`, narrowed to the three columns the ranking needs.
/// The file carries 25 more; the csv reader matches on the header name and
/// drops the rest. Both score columns are optional because the export leaves
/// cells blank for a model it has not scored.
#[derive(Debug, Deserialize)]
struct ModelRow {
    slug: String,
    intelligence: Option<f64>,
    intelligence_rank: Option<u32>,
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

/// What one run did, for the summary table. Holds counts and variable names,
/// never a key.
#[derive(Debug, Default)]
struct Report {
    /// Provider name, its `.env` variable, how many keys it has.
    providers: Vec<(String, String, usize)>,
    ranked_models: usize,
    skipped_models: usize,
    capability_rows: usize,
}

impl Report {
    fn total_keys(&self) -> usize {
        self.providers.iter().map(|(_, _, n)| n).sum()
    }

    fn render(&self, out: &Path) -> String {
        let mut keys = Table::new();
        keys.set_header(vec!["provider", "env variable", "keys"]);
        for (name, env, count) in &self.providers {
            keys.add_row(vec![name.clone(), env.clone(), count.to_string()]);
        }

        let mut totals = Table::new();
        totals.set_header(vec!["what", "count"]);
        for (what, count) in [
            ("providers", self.providers.len()),
            ("keys", self.total_keys()),
            ("ranked models", self.ranked_models),
            ("models.csv rows with no score", self.skipped_models),
            ("capability rows", self.capability_rows),
        ] {
            totals.add_row(vec![what.to_string(), count.to_string()]);
        }

        format!("{keys}\n{totals}\nWritten to {}\n", out.display())
    }
}

/// Reads the source data and writes the six files. Returns the counts for the
/// summary.
fn generate(source: &Path, out: &Path, force: bool) -> anyhow::Result<Report> {
    fs::create_dir_all(out).with_context(|| format!("creating {}", out.display()))?;

    let env_path = out.join(".env");
    if env_path.exists() && !force {
        bail!(
            "{} already exists. It holds live keys, so this will not overwrite it. \
             Pass --force to replace it.",
            env_path.display()
        );
    }

    let providers = read_providers(&source.join("providers.yaml"))?;
    let mut report = Report::default();

    // .env first: if the key material cannot be written there is no point
    // writing a providers.yml that points at variables which do not exist.
    let mut env_body = String::from(
        "# Provider API keys, comma separated, one variable per provider.\n\
         # Generated by llmrouter-make-config. Never commit this file.\n",
    );
    let mut seen = HashSet::new();
    for provider in &providers {
        let env_name = keys_env_name(&provider.name);
        if !seen.insert(env_name.clone()) {
            bail!(
                "two providers map to the same .env variable {env_name}; \
                 rename one of them in providers.yaml"
            );
        }
        env_body.push_str(&format!("{env_name}={}\n", provider.keys.join(",")));
        report
            .providers
            .push((provider.name.clone(), env_name, provider.keys.len()));
    }
    write_private(&env_path, &env_body)?;

    write_yaml(
        &out.join("providers.yml"),
        "# Channels: name, wire format, base URL, and the .env variable holding\n\
         # that provider's keys. Safe to read and share: it names variables\n\
         # rather than holding keys. Generated by llmrouter-make-config.\n",
        &providers
            .iter()
            .zip(&report.providers)
            .map(|(p, (_, env_name, _))| Provider {
                name: p.name.clone(),
                wire: p.wire,
                base_url: p.base_url.trim_end_matches('/').to_string(),
                keys_env: env_name.clone(),
                models_path: p
                    .models_path
                    .clone()
                    .unwrap_or_else(|| "/models".to_string()),
            })
            .collect::<Vec<_>>(),
    )?;

    let (ranking, skipped) = read_ranking(&source.join("models.csv"))?;
    report.ranked_models = ranking.len();
    report.skipped_models = skipped;
    write_yaml(
        &out.join("intelligence.yml"),
        "# The Artificial Analysis ranking, from models.csv. Lower\n\
         # intelligence_rank is better. Generated by llmrouter-make-config.\n",
        &ranking,
    )?;

    fs::write(out.join("models.yml"), MODELS_YML).context("writing models.yml")?;
    fs::write(out.join("router.yml"), router_yml()?).context("writing router.yml")?;

    report.capability_rows = copy_capabilities(
        &source.join("model_capabilities.csv"),
        &out.join("model_capabilities.csv"),
    )?;

    Ok(report)
}

/// The `.env` variable name for a provider: the name uppercased, every
/// non-alphanumeric character turned into an underscore, then `_KEYS`.
/// `cloudflare-b4b050cc` becomes `CLOUDFLARE_B4B050CC_KEYS`.
fn keys_env_name(provider: &str) -> String {
    let stem: String = provider
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    format!("{stem}_KEYS")
}

fn read_providers(path: &Path) -> anyhow::Result<Vec<SourceProvider>> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let parsed: SourceFile =
        serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    Ok(parsed.providers)
}

/// Reads `models.csv` into the ranking. A row with no intelligence score is
/// skipped and counted: it has nothing to rank by. Returns the entries and
/// the number skipped.
fn read_ranking(path: &Path) -> anyhow::Result<(Vec<IntelligenceEntry>, usize)> {
    let mut reader =
        csv::Reader::from_path(path).with_context(|| format!("reading {}", path.display()))?;
    let mut out = Vec::new();
    let mut skipped = 0_usize;
    for row in reader.deserialize::<ModelRow>() {
        let row = row.with_context(|| format!("parsing a row of {}", path.display()))?;
        match (row.intelligence, row.intelligence_rank) {
            (Some(score), Some(rank)) if score.is_finite() && score >= 0.0 => {
                out.push(IntelligenceEntry {
                    slug: row.slug,
                    intelligence: score.round().min(f64::from(u16::MAX)) as u16,
                    intelligence_rank: rank,
                });
            }
            _ => skipped += 1,
        }
    }
    Ok((out, skipped))
}

/// Copies the capabilities file across unchanged and returns its row count.
fn copy_capabilities(from: &Path, to: &Path) -> anyhow::Result<usize> {
    fs::copy(from, to)
        .with_context(|| format!("copying {} to {}", from.display(), to.display()))?;
    let mut reader =
        csv::Reader::from_path(to).with_context(|| format!("reading {}", to.display()))?;
    Ok(reader.records().flatten().count())
}

/// Serialises `value` to YAML under a header comment. serde_yaml owns the
/// quoting and escaping; nothing here formats YAML by hand.
fn write_yaml<T: serde::Serialize>(path: &Path, header: &str, value: &T) -> anyhow::Result<()> {
    let body =
        serde_yaml::to_string(value).with_context(|| format!("serialising {}", path.display()))?;
    fs::write(path, format!("{header}{body}"))
        .with_context(|| format!("writing {}", path.display()))
}

/// Writes a file only the owner can read. Created at mode 0600 rather than
/// chmod'ed afterwards, so it is never briefly world readable.
fn write_private(path: &Path, contents: &str) -> anyhow::Result<()> {
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
    let mut file = opts
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    file.write_all(contents.as_bytes())
        .with_context(|| format!("writing {}", path.display()))
}

/// `models.yml` starts empty. The catalogue works the context window, output
/// limit, tool support and ranking out on its own; this file is only for the
/// cases where it gets one wrong.
const MODELS_YML: &str = "\
# Per-model corrections. Empty on purpose.
#
# The catalogue takes each model's context window, output limit and tool
# support from the provider's own /models response joined against
# model_capabilities.csv, and its ranking by matching the model id against
# intelligence.yml. This file overrides any of those, per provider and model:
#
#   - provider: zai
#     model_id: glm-5.3
#     limit_context: 200000
#     limit_output: 128000
#     tool_call: true
#     ranks_as: glm-5-3        # the Artificial Analysis slug to rank under
#
# An entry is only needed where the automatic match is wrong. A live run logs
# every loose ranking match at startup, so start from that list rather than
# guessing: those are the models that need an entry here.
[]
";

/// The header on `router.yml`, saying which values came from where.
const ROUTER_YML_HEADER: &str = "\
# router.yml, generated by llmrouter-make-config.
#
# Every setting is written out explicitly rather than left to the code's
# defaults, so an operator can see what is in force and change it. Restart the
# router to apply a change.
#
# Values come from DESIGN.md's `router.yml` block. Two settings are not in that
# block and come from the Default impl in src/config.rs instead:
#
#   listen             0.0.0.0:8080
#   cache.page_cache_bytes  256000000, a cap on redb's own page cache. redb
#                      defaults to 1GiB, which on its own would push the
#                      process past memory_high_water.
#
# One setting where the two disagree, with DESIGN.md's figure used here:
#
#   memory_high_water  DESIGN.md says 3GB, which the loader reads as
#                      3,000,000,000. The Default impl uses 3 GiB,
#                      3,221,225,472. This file carries DESIGN.md's figure,
#                      which is the smaller and therefore safer gate.
#
# Byte sizes are written as plain integers. The loader also accepts a suffixed
# form such as 3GB, 100GB or 128KiB, so either is fine to hand edit.
";

/// `router.yml`, built from `RouterConfig::default()` so the file can never
/// drift from what the code reads, with DESIGN.md's `memory_high_water`
/// applied over it.
fn router_yml() -> anyhow::Result<String> {
    let config = RouterConfig {
        memory_high_water: 3_000_000_000,
        ..Default::default()
    };
    let body = serde_yaml::to_string(&config).context("serialising router.yml")?;
    Ok(format!("{ROUTER_YML_HEADER}{body}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROVIDERS_YAML: &str = "\
providers:
  - name: zai
    wire: openai
    base_url: https://api.z.ai/api/coding/paas/v4/
    priority: 100
    known_models: [glm-5.3]
    keys:
      - key-one
      - key-two
  - name: cloudflare-b4b050cc
    wire: openai
    base_url: https://api.cloudflare.com/client/v4/accounts/b4b050cc/ai/v1
    priority: 38
    known_models: ['@cf/meta/llama']
    keys: []
  - name: anth
    wire: anthropic
    base_url: https://api.anthropic.com
    priority: 10
    known_models: []
    models_path: /v1/models
    keys:
      - key-three
";

    const MODELS_CSV: &str = "\
slug,name,intelligence,coding_index,aa_id,intelligence_rank
claude-fable-5-1,Claude Fable 5.1,53.4,81.6,abc,1
gemini-3-8-flash,Gemini 3.8 Flash,40.2,70.0,def,7
no-score-model,Unscored,,,ghi,
";

    const CAPABILITIES_CSV: &str = "\
provider,model_id,tool_call,reasoning,structured_output,limit_context,limit_output,modalities_input,family,open_weights
302ai,MiniMax-M1,1,0,,1000000,128000,\"[\"\"text\"\"]\",minimax,0
gemini,gemini-3.8-flash,0,0,,1048576,,\"[\"\"text\"\"]\",,0
";

    /// Writes the three source files into a throwaway directory.
    fn source_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("a throwaway directory");
        fs::write(dir.path().join("providers.yaml"), PROVIDERS_YAML).unwrap();
        fs::write(dir.path().join("models.csv"), MODELS_CSV).unwrap();
        fs::write(dir.path().join("model_capabilities.csv"), CAPABILITIES_CSV).unwrap();
        dir
    }

    #[test]
    fn env_variable_names_flatten_everything_that_is_not_alphanumeric() {
        assert_eq!(keys_env_name("zai"), "ZAI_KEYS");
        assert_eq!(keys_env_name("hades-local"), "HADES_LOCAL_KEYS");
        assert_eq!(
            keys_env_name("cloudflare-b4b050cc"),
            "CLOUDFLARE_B4B050CC_KEYS"
        );
        assert_eq!(keys_env_name("a.b c/d"), "A_B_C_D_KEYS");
    }

    #[test]
    fn a_trailing_slash_comes_off_the_base_url() {
        let source = source_dir();
        let out = tempfile::tempdir().unwrap();
        generate(source.path(), out.path(), false).expect("generates");

        let providers: Vec<Provider> =
            serde_yaml::from_str(&fs::read_to_string(out.path().join("providers.yml")).unwrap())
                .expect("providers.yml parses");
        assert_eq!(providers[0].base_url, "https://api.z.ai/api/coding/paas/v4");
        assert_eq!(providers[0].keys_env, "ZAI_KEYS");
        // No models_path in the source means the router's own default.
        assert_eq!(providers[0].models_path, "/models");
        // One that sets it keeps what it set, and the wire name survives.
        assert_eq!(providers[2].models_path, "/v1/models");
        assert_eq!(providers[2].wire, WireName::Anthropic);
    }

    #[test]
    fn a_provider_with_no_keys_still_gets_its_variable() {
        let source = source_dir();
        let out = tempfile::tempdir().unwrap();
        let report = generate(source.path(), out.path(), false).expect("generates");

        assert_eq!(report.providers.len(), 3);
        assert_eq!(report.total_keys(), 3);
        assert_eq!(
            report.providers[1],
            (
                "cloudflare-b4b050cc".to_string(),
                "CLOUDFLARE_B4B050CC_KEYS".to_string(),
                0
            )
        );

        let env = fs::read_to_string(out.path().join(".env")).unwrap();
        assert!(env.contains("CLOUDFLARE_B4B050CC_KEYS=\n"));
        // The router reads the empty variable back as no keys, not as a key
        // that happens to be the empty string.
        let parsed = llmrouter::config::parse_env_file(&env);
        assert!(parsed["CLOUDFLARE_B4B050CC_KEYS"].is_empty());
        assert_eq!(parsed["ZAI_KEYS"].len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn the_env_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let source = source_dir();
        let out = tempfile::tempdir().unwrap();
        generate(source.path(), out.path(), false).expect("generates");
        let mode = fs::metadata(out.path().join(".env"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn a_blank_intelligence_cell_is_skipped_and_counted() {
        let source = source_dir();
        let out = tempfile::tempdir().unwrap();
        let report = generate(source.path(), out.path(), false).expect("generates");

        assert_eq!(report.ranked_models, 2);
        assert_eq!(report.skipped_models, 1);

        let ranking: Vec<IntelligenceEntry> =
            serde_yaml::from_str(&fs::read_to_string(out.path().join("intelligence.yml")).unwrap())
                .expect("intelligence.yml parses");
        assert_eq!(ranking.len(), 2);
        // 53.4 rounds to 53, the way the loader's own score parser does.
        assert_eq!(ranking[0].slug, "claude-fable-5-1");
        assert_eq!(ranking[0].intelligence, 53);
        assert_eq!(ranking[0].intelligence_rank, 1);
        assert!(!ranking.iter().any(|e| e.slug == "no-score-model"));
    }

    #[test]
    fn an_existing_env_is_not_overwritten_without_force() {
        let source = source_dir();
        let out = tempfile::tempdir().unwrap();
        fs::write(out.path().join(".env"), "HAND_EDITED_KEYS=keep-me\n").unwrap();

        let err = generate(source.path(), out.path(), false).expect_err("refuses");
        assert!(err.to_string().contains("--force"), "{err}");
        assert_eq!(
            fs::read_to_string(out.path().join(".env")).unwrap(),
            "HAND_EDITED_KEYS=keep-me\n"
        );
        // Nothing else was written either: the run stops before any of it.
        assert!(!out.path().join("providers.yml").exists());

        generate(source.path(), out.path(), true).expect("--force replaces it");
        assert!(fs::read_to_string(out.path().join(".env"))
            .unwrap()
            .contains("ZAI_KEYS="));
    }

    #[test]
    fn two_providers_cannot_share_one_variable() {
        let source = tempfile::tempdir().unwrap();
        fs::write(
            source.path().join("providers.yaml"),
            "providers:\n\
             \x20 - name: a-b\n    wire: openai\n    base_url: https://x.invalid\n    keys: [one]\n\
             \x20 - name: a.b\n    wire: openai\n    base_url: https://y.invalid\n    keys: [two]\n",
        )
        .unwrap();
        let out = tempfile::tempdir().unwrap();
        let err = generate(source.path(), out.path(), false).expect_err("refuses");
        assert!(err.to_string().contains("A_B_KEYS"), "{err}");
    }

    #[test]
    fn the_output_directory_is_created_and_holds_all_six_files() {
        let source = source_dir();
        let parent = tempfile::tempdir().unwrap();
        let out = parent.path().join("nested").join("config");
        let report = generate(source.path(), &out, false).expect("generates");

        for name in [
            ".env",
            "providers.yml",
            "models.yml",
            "intelligence.yml",
            "router.yml",
            "model_capabilities.csv",
        ] {
            assert!(out.join(name).exists(), "{name} should have been written");
        }
        assert_eq!(report.capability_rows, 2);
        // The capabilities file goes across byte for byte.
        assert_eq!(
            fs::read_to_string(out.join("model_capabilities.csv")).unwrap(),
            CAPABILITIES_CSV
        );
        assert!(report.render(&out).contains("capability rows"));
    }

    /// The whole point of the output: the router's own loader reads it back.
    #[test]
    fn the_router_loads_what_this_writes() {
        let source = source_dir();
        let out = tempfile::tempdir().unwrap();
        generate(source.path(), out.path(), false).expect("generates");

        let config = llmrouter::Config::load(out.path()).expect("the router loads it");
        assert_eq!(config.providers.len(), 3);
        assert!(config.models.is_empty());
        assert_eq!(config.intelligence.len(), 2);
        assert_eq!(config.keys_for(&config.providers[0]).len(), 2);
        assert_eq!(
            config.capability("302ai", "minimax-m1").unwrap().tool_call,
            Some(true)
        );
        assert_eq!(
            config.rank_of("gemini", "gemini-3.8-flash").0,
            40,
            "the ranking joins against the generated intelligence.yml"
        );
    }

    /// `router.yml` must survive the round trip. `RouterConfig` sets
    /// `deny_unknown_fields`, so a field that serialises under a name the
    /// loader does not read would fail the boot rather than be ignored.
    #[test]
    fn router_yml_round_trips_and_carries_the_design_values() {
        let text = router_yml().expect("serialises");
        assert!(text.starts_with('#'), "the header comment comes first");

        let parsed: RouterConfig = serde_yaml::from_str(&text).expect("router.yml parses back");
        let defaults = RouterConfig::default();
        assert_eq!(parsed.race_width, 32);
        assert_eq!(parsed.max_per_channel, 2);
        assert_eq!(parsed.hedge_delay_ms, 0);
        assert!(parsed.streaming);
        assert_eq!(parsed.hedge_window_ms, 1000);
        assert_eq!(parsed.request_timeout, defaults.request_timeout);
        assert_eq!(parsed.connect_timeout, defaults.connect_timeout);
        assert_eq!(parsed.stream_stall_timeout, defaults.stream_stall_timeout);
        assert_eq!(parsed.keepalive_interval, defaults.keepalive_interval);
        assert_eq!(parsed.spill_above_concurrent, 100);
        assert_eq!(parsed.listen, defaults.listen);
        assert_eq!(parsed.http2_stream_window, 131_072);
        assert_eq!(parsed.http2_connection_window, 1_048_576);
        assert_eq!(
            parsed.health_poll.backoff_max,
            defaults.health_poll.backoff_max
        );
        assert_eq!(
            parsed.health_poll.probe_timeout,
            defaults.health_poll.probe_timeout
        );
        assert_eq!(parsed.cache.max_bytes, defaults.cache.max_bytes);
        assert_eq!(parsed.cache.ttl, defaults.cache.ttl);
        assert_eq!(
            parsed.cache.page_cache_bytes,
            defaults.cache.page_cache_bytes
        );
        // DESIGN.md's 3GB, not the Default impl's 3 GiB.
        assert_eq!(parsed.memory_high_water, 3_000_000_000);
        assert_ne!(parsed.memory_high_water, defaults.memory_high_water);
    }

    #[test]
    fn models_yml_is_an_empty_list_the_loader_accepts() {
        let out = tempfile::tempdir().unwrap();
        fs::write(out.path().join("models.yml"), MODELS_YML).unwrap();
        let config = llmrouter::Config::load(out.path()).expect("loads");
        assert!(config.models.is_empty());
    }

    #[test]
    fn a_missing_source_file_says_which_one() {
        let empty = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        let err = generate(empty.path(), out.path(), false).expect_err("no providers.yaml");
        assert!(err.to_string().contains("providers.yaml"), "{err}");
    }

    #[test]
    fn the_arguments_default_to_the_deploy_path() {
        let args = Args::parse_from(["llmrouter-make-config"]);
        assert_eq!(args.out, PathBuf::from(DEFAULT_OUT));
        assert_eq!(args.source, PathBuf::from("."));
        assert!(!args.force);

        let args = Args::parse_from([
            "llmrouter-make-config",
            "--source",
            "/data",
            "--out",
            "/tmp/cfg",
            "--force",
        ]);
        assert_eq!(args.source, PathBuf::from("/data"));
        assert_eq!(args.out, PathBuf::from("/tmp/cfg"));
        assert!(args.force);
    }
}
