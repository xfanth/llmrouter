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

/// Where the router reads its config in production: the host path
/// docker-compose bind mounts at `/config`, so it only exists on the deploy
/// host. Anywhere else, set `LLMROUTER_CONFIG_DIR` or pass `--out`.
///
/// This directory is INSIDE the git checkout. On the deploy host
/// `/mnt/shuttle/share/app-data/llmrouter` is the repo's main working tree,
/// so this is its `config/` folder, and the `.env` written here holds every
/// live key. Only the `config/` and `.env` lines in `.gitignore` keep git
/// from seeing it. Never `git add -f` anything under it.
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
    /// Must not be the source directory: the run refuses rather than
    /// overwrite the source `model_capabilities.csv`.
    /// Reads `LLMROUTER_CONFIG_DIR`, the same variable the router itself uses
    /// to find its config, so off the deploy host both tools point at one
    /// directory without a flag.
    #[arg(long, env = "LLMROUTER_CONFIG_DIR", default_value = DEFAULT_OUT)]
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

/// One provider in `providers.yaml`. `priority` is read and dropped:
/// `providers.yml` has no field for it. `known_models` is carried across as
/// `models` only for a provider with no model-listing endpoint, since the
/// probe treats that list as a pass on a 404 or 405.
/// Unknown keys are ignored rather than fatal, since the file is generated
/// upstream and may grow columns.
#[derive(Debug, Deserialize)]
struct SourceProvider {
    name: String,
    wire: WireName,
    base_url: String,
    #[serde(default)]
    keys: Vec<String>,
    #[serde(default)]
    models_path: Option<String>,
    #[serde(default)]
    known_models: Vec<String>,
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
    #[serde(default)]
    release_date: Option<String>,
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

/// What one run did, for the summary table. Holds counts and variable names,
/// never a key.
#[derive(Debug, Default)]
struct Report {
    /// Provider name, its `.env` variable, how many keys it has, and whether
    /// its base URL is only reachable from the host it runs on.
    providers: Vec<(String, String, usize, bool)>,
    ranked_models: usize,
    skipped_models: usize,
    capability_rows: usize,
    /// Files that already existed and were left as the operator had them.
    kept: Vec<String>,
}

impl Report {
    fn total_keys(&self) -> usize {
        self.providers.iter().map(|(_, _, n, _)| n).sum()
    }

    /// The providers whose base URL is a LAN or loopback address.
    fn lan_only(&self) -> Vec<&str> {
        self.providers
            .iter()
            .filter(|(_, _, _, lan)| *lan)
            .map(|(name, _, _, _)| name.as_str())
            .collect()
    }

    fn render(&self, out: &Path) -> String {
        let mut keys = Table::new();
        keys.set_header(vec!["provider", "env variable", "keys", "reachable from"]);
        for (name, env, count, lan) in &self.providers {
            let scope = if *lan { "its own host" } else { "anywhere" };
            keys.add_row(vec![
                name.clone(),
                env.clone(),
                count.to_string(),
                scope.to_string(),
            ]);
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

        let lan = self.lan_only();
        let note = if lan.is_empty() {
            String::new()
        } else {
            format!(
                "\nLAN or loopback only: {}. The router's keyless reachability\n\
                 probe fails on these from any other machine, so they are dropped\n\
                 at startup off their own host. Expected there, not a fault.\n",
                lan.join(", ")
            )
        };

        let kept = if self.kept.is_empty() {
            String::new()
        } else {
            format!(
                "\nLeft as they were: {}. These hold hand corrections the source\n\
                 data knows nothing about, so an existing one is never replaced.\n\
                 Delete it and rerun to get a fresh template.\n",
                self.kept.join(", ")
            )
        };

        format!(
            "{keys}\n{totals}\n{note}{kept}Written to {}\n",
            out.display()
        )
    }
}

/// Reads the source data and writes the six files. Returns the counts for the
/// summary.
fn generate(source: &Path, out: &Path, force: bool) -> anyhow::Result<Report> {
    // The source model_capabilities.csv is a tracked, dated snapshot. Writing
    // the aliased copy over it would edit the source, and the next run would
    // then treat the stale alias rows as source data. Canonicalising catches
    // `--out .` from the data directory as well as a symlinked output file.
    let source_caps = fs::canonicalize(source.join("model_capabilities.csv")).ok();
    if source_caps.is_some()
        && source_caps == fs::canonicalize(out.join("model_capabilities.csv")).ok()
    {
        bail!(
            "--out {} would overwrite the source model_capabilities.csv in {}; \
             write the config to a different directory",
            out.display(),
            source.display()
        );
    }

    let env_path = out.join(".env");
    if env_path.exists() && !force {
        bail!(
            "{} already exists. It holds live keys, so this will not overwrite it. \
             Pass --force to replace it.",
            env_path.display()
        );
    }

    // Every source file is read and every output body built before anything
    // is written. A failure part way through used to leave .env and
    // providers.yml on disk with the other four files missing, and the guard
    // above then refused the retry as though an operator had put that .env
    // there.
    let providers = read_providers(&source.join("providers.yaml"))?;
    let mut report = Report::default();

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
        report.providers.push((
            provider.name.clone(),
            env_name,
            provider.keys.len(),
            is_lan_only(&provider.base_url),
        ));
    }
    let providers_yml = yaml_text(
        "providers.yml",
        "# Channels: name, wire format, base URL, and the .env variable holding\n\
         # that provider's keys. Safe to read and share: it names variables\n\
         # rather than holding keys. Generated by llmrouter-make-config.\n",
        &providers
            .iter()
            .zip(&report.providers)
            .map(|(p, (_, env_name, _, _))| Provider {
                name: p.name.clone(),
                wire: p.wire,
                base_url: fix_base_url(&p.base_url),
                keys_env: env_name.clone(),
                models_path: p
                    .models_path
                    .clone()
                    .unwrap_or_else(|| "/models".to_string()),
                models: if has_no_model_listing(&p.name) {
                    p.known_models.clone()
                } else {
                    Vec::new()
                },
            })
            .collect::<Vec<_>>(),
    )?;

    let (ranking, skipped) = read_ranking(&source.join("models.csv"))?;
    report.ranked_models = ranking.len();
    report.skipped_models = skipped;
    let intelligence_yml = yaml_text(
        "intelligence.yml",
        "# The Artificial Analysis ranking, from models.csv. Lower\n\
         # intelligence_rank is better. Generated by llmrouter-make-config.\n",
        &ranking,
    )?;

    let (capabilities, capability_rows) = read_capabilities(
        &source.join("model_capabilities.csv"),
        &providers.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
    )?;
    report.capability_rows = capability_rows;

    let router_yml = router_yml()?;

    // Every source file has now been read and every body built. Only now does
    // anything land on disk. Only the last path segment is created. The
    // default --out sits on a network share, and if that share is not mounted
    // a create_dir_all would quietly build the tree on the local disk under
    // the mount point and put the live keys there. A missing parent is an
    // error instead.
    if !out.is_dir() {
        fs::create_dir(out).with_context(|| {
            format!(
                "creating {}. Its parent must already exist; if it is on a share, \
                 check the share is mounted",
                out.display()
            )
        })?;
    }
    write_private(&env_path, &env_body)?;
    fs::write(out.join("providers.yml"), providers_yml).context("writing providers.yml")?;
    fs::write(out.join("intelligence.yml"), intelligence_yml)
        .context("writing intelligence.yml")?;
    fs::write(out.join("model_capabilities.csv"), capabilities)
        .context("writing model_capabilities.csv")?;

    // models.yml and router.yml are the operator's edit surface. The source
    // data says nothing about what is in them, so a rerun leaves an existing
    // one alone and says so in the summary. --force covers the .env, which is
    // generated material; it does not licence throwing away hand corrections.
    for (name, body) in [("models.yml", MODELS_YML), ("router.yml", &router_yml)] {
        let path = out.join(name);
        if path.exists() {
            report.kept.push(name.to_string());
        } else {
            fs::write(&path, body).with_context(|| format!("writing {name}"))?;
        }
    }

    Ok(report)
}

/// Base URLs that `build_providers.py` copies out of new-api's `channels.csv`
/// without their version segment. The router appends `models_path` to the base
/// URL exactly as given, so a missing `/v1` turns every keyed `/models` probe
/// into a 404 and the provider contributes no channels at all.
///
/// An explicit list rather than a rule: most providers in `providers.yaml`
/// already carry a version segment, and several of those are not `/v1` (`/v4`,
/// `/v1beta/openai`, `/ai/v1`), so appending one by guesswork would break more
/// than it fixed. Add a line here when a live run shows a keyed `/models` probe
/// returning 404 on a provider that is otherwise alive.
const BASE_URL_FIXUPS: &[(&str, &str)] = &[
    (
        "https://api.groq.com/openai",
        "https://api.groq.com/openai/v1",
    ),
    ("https://api.mistral.ai", "https://api.mistral.ai/v1"),
];

/// Providers with no model-listing endpoint, whose `known_models` therefore
/// has to be written into `providers.yml` as the probe's fallback catalogue.
/// Cloudflare Workers AI is the only one: its OpenAI-compatible path takes
/// POST only, so the keyed `/models` probe answers 405.
///
/// Every other provider lists its models, so it gets no fallback. A 404 or
/// 405 there means the listing path is wrong or has moved, and the pair must
/// be marked unhealthy and reported on `/healthz` rather than quietly coming
/// up with a one-to-three-model catalogue. That is also the signal
/// `BASE_URL_FIXUPS` above is written from.
fn has_no_model_listing(provider: &str) -> bool {
    provider.starts_with("cloudflare-")
}

/// Trims any trailing slash, then applies `BASE_URL_FIXUPS`.
fn fix_base_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    BASE_URL_FIXUPS
        .iter()
        .find(|(from, _)| *from == trimmed)
        .map_or_else(|| trimmed.to_string(), |(_, to)| (*to).to_string())
}

/// The `.env` variable name for a provider: the name uppercased, every
/// non-alphanumeric character turned into an underscore, then `_KEYS`.
/// `cloudflare-b4b050cc` becomes `CLOUDFLARE_B4B050CC_KEYS`.
///
/// A name starting with a digit, such as the new-api channel `302ai`, gets a
/// leading underscore: `dotenvy` rejects an identifier starting with a digit,
/// so the router would drop the line and leave that provider with no keys.
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
    let prefix = if stem.starts_with(|c: char| c.is_ascii_digit()) {
        "_"
    } else {
        ""
    };
    format!("{prefix}{stem}_KEYS")
}

/// True when a base URL names a host only resolvable on the machine the
/// provider itself runs on: localhost, a loopback, private, shared (CGNAT or
/// tailnet), unique-local or link-local address, a bare hostname, or a
/// `.arpa`, `.local`, `.lan`, `.home` or `.internal` name. The router's keyless reachability probe fails on these
/// from anywhere else and drops the provider, which is expected rather than a
/// broken config, so the summary says which ones they are. A URL that will not
/// parse is treated as public: the router reports the real problem.
fn is_lan_only(base_url: &str) -> bool {
    use reqwest::Url;

    let Ok(url) = Url::parse(base_url) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_matches(['[', ']']).trim_end_matches('.');

    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return match ip {
            // 100.64.0.0/10 is the shared address space a CGNAT or a tailnet
            // hands out. `Ipv4Addr::is_shared` is still unstable, so test it here.
            std::net::IpAddr::V4(ip) => {
                let o = ip.octets();
                ip.is_loopback()
                    || ip.is_private()
                    || ip.is_link_local()
                    || (o[0] == 100 && (64..=127).contains(&o[1]))
            }
            // fc00::/7 unique-local and fe80::/10 link-local. The matching
            // `Ipv6Addr` helpers are unstable, so test the first segment here.
            std::net::IpAddr::V6(ip) => {
                let first = ip.segments()[0];
                ip.is_loopback() || (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80
            }
        };
    }

    let host = host.to_ascii_lowercase();
    host == "localhost"
        || !host.contains('.')
        || [".arpa", ".local", ".lan", ".home", ".internal"]
            .iter()
            .any(|suffix| host.ends_with(suffix))
}

/// Reads `providers.yaml`. The parse error text is never surfaced, because
/// serde_yaml quotes the offending value verbatim and that value may be a
/// key. Only the location it points at is reported, the same rule
/// `parse_env_file` in `src/config.rs` follows for dotenvy.
fn read_providers(path: &Path) -> anyhow::Result<Vec<SourceProvider>> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let parsed: SourceFile = serde_yaml::from_str(&text).map_err(|e| {
        let where_ = e.location().map_or_else(
            || "an unknown position".to_string(),
            |l| format!("line {} column {}", l.line(), l.column()),
        );
        anyhow::anyhow!(
            "{} is malformed at {where_} (the parser's message is withheld because it quotes the offending value, which may be a key)",
            path.display()
        )
    })?;
    Ok(parsed.providers)
}

/// Reads `models.csv` into the ranking. A row with no intelligence score is
/// skipped and counted: it has nothing to rank by. Returns the entries and
/// the number skipped.
///
/// The score columns are `Option`, so serde would fill a renamed or dropped
/// column with `None` and every row would quietly be skipped. The header is
/// checked up front instead, and a file where no row ranks at all is an
/// error, since an empty ranking loses the sort order for every request.
fn read_ranking(path: &Path) -> anyhow::Result<(Vec<IntelligenceEntry>, usize)> {
    let mut reader =
        csv::Reader::from_path(path).with_context(|| format!("reading {}", path.display()))?;
    let headers = reader
        .headers()
        .with_context(|| format!("reading the header of {}", path.display()))?;
    for column in ["slug", "intelligence", "intelligence_rank"] {
        anyhow::ensure!(
            headers.iter().any(|h| h == column),
            "{} has no {column} column",
            path.display()
        );
    }
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
                    release_date: row.release_date.filter(|d| !d.trim().is_empty()),
                });
            }
            _ => skipped += 1,
        }
    }
    anyhow::ensure!(
        !out.is_empty(),
        "{} has no row with both an intelligence score and a rank",
        path.display()
    );
    Ok((out, skipped))
}

/// Capability rows that arrive under a provider name no entry in
/// `providers.yaml` uses, paired with the provider-name prefix they belong to.
/// `model_capabilities.csv` carries its 27 Cloudflare Workers AI rows under
/// `cloudflare-workers-ai`, while the live `providers.yaml` names each account
/// `cloudflare-<account id>`. `Config::capability` joins on the provider name
/// exactly, so those rows match nothing and every Cloudflare channel falls
/// back to the 8,192 token default context window, which drops it from any
/// prompt longer than that. Add a line here when an export names a provider
/// differently from the key file.
const CAPABILITY_ALIASES: &[(&str, &str)] = &[("cloudflare-workers-ai", "cloudflare-")];

/// Reads the capabilities file into memory and returns its bytes and row
/// count. Read rather than copied so that the whole output directory is
/// written in one go, after every source file has been read.
///
/// The source rows go across unchanged. Appended after them is a copy of each
/// `CAPABILITY_ALIASES` row under the name of every provider that needs it, so
/// the join in `src/config.rs` finds the real context window instead of the
/// default. A provider the file already carries rows for is left alone.
fn read_capabilities(from: &Path, providers: &[String]) -> anyhow::Result<(Vec<u8>, usize)> {
    let mut bytes = fs::read(from).with_context(|| format!("reading {}", from.display()))?;

    let mut reader = csv::Reader::from_reader(bytes.as_slice());
    let provider_column = reader
        .headers()
        .with_context(|| format!("reading the header of {}", from.display()))?
        .iter()
        .position(|h| h == "provider")
        .with_context(|| format!("{} has no provider column", from.display()))?;
    let rows: Vec<csv::StringRecord> = reader.records().flatten().collect();
    let known: HashSet<&str> = rows.iter().filter_map(|r| r.get(provider_column)).collect();

    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    let mut aliased = 0_usize;
    for (source_name, prefix) in CAPABILITY_ALIASES {
        let targets: Vec<&str> = providers
            .iter()
            .map(String::as_str)
            .filter(|name| name.starts_with(prefix) && !known.contains(name))
            .collect();
        for row in rows
            .iter()
            .filter(|r| r.get(provider_column) == Some(*source_name))
        {
            for target in &targets {
                let mut fields: Vec<&str> = row.iter().collect();
                fields[provider_column] = target;
                writer
                    .write_record(&fields)
                    .with_context(|| format!("aliasing a row of {}", from.display()))?;
                aliased += 1;
            }
        }
    }
    let extra = writer
        .into_inner()
        .with_context(|| format!("aliasing rows of {}", from.display()))?;

    if !extra.is_empty() {
        // An export with no final newline would otherwise run the first
        // appended row onto the end of the last source one.
        if !bytes.ends_with(b"\n") {
            bytes.push(b'\n');
        }
        bytes.extend_from_slice(&extra);
    }

    let total = rows.len() + aliased;
    Ok((bytes, total))
}

/// Serialises `value` to YAML under a header comment. serde_yaml owns the
/// quoting and escaping; nothing here formats YAML by hand.
fn yaml_text<T: serde::Serialize>(what: &str, header: &str, value: &T) -> anyhow::Result<String> {
    let body = serde_yaml::to_string(value).with_context(|| format!("serialising {what}"))?;
    Ok(format!("{header}{body}"))
}

/// Writes a file only the owner can read. A new file is created at mode 0600,
/// so it is never briefly world readable. An existing file keeps whatever mode
/// it already had, because the open mode only applies on creation, so it is
/// narrowed to 0600 before anything is written into it.
fn write_private(path: &Path, contents: &str) -> anyhow::Result<()> {
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
    let mut file = opts
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    #[cfg(unix)]
    file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))
        .with_context(|| format!("restricting permissions on {}", path.display()))?;
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
cloudflare-workers-ai,@cf/meta/llama,1,0,,128000,4096,\"[\"\"text\"\"]\",llama,1
";

    /// Writes the three source files into a throwaway directory.
    fn source_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("a throwaway directory");
        fs::write(dir.path().join("providers.yaml"), PROVIDERS_YAML).unwrap();
        fs::write(dir.path().join("models.csv"), MODELS_CSV).unwrap();
        fs::write(dir.path().join("model_capabilities.csv"), CAPABILITIES_CSV).unwrap();
        dir
    }

    /// A run that fails because a source file is missing must leave the output
    /// directory exactly as it found it, so the retry after the missing file is
    /// copied across is not refused by the `.env` guard.
    #[test]
    fn a_failed_run_writes_nothing_and_the_retry_succeeds() {
        let source = tempfile::tempdir().unwrap();
        fs::write(source.path().join("providers.yaml"), PROVIDERS_YAML).unwrap();
        let out = tempfile::tempdir().unwrap();

        let err = generate(source.path(), out.path(), false).expect_err("no models.csv");
        assert!(err.to_string().contains("models.csv"), "{err}");
        assert_eq!(
            fs::read_dir(out.path()).unwrap().count(),
            0,
            "a failed run leaves nothing behind, not even .env"
        );

        fs::write(source.path().join("models.csv"), MODELS_CSV).unwrap();
        let out2 = tempfile::tempdir().unwrap();
        let err = generate(source.path(), out2.path(), false).expect_err("no capabilities csv");
        assert!(err.to_string().contains("model_capabilities.csv"), "{err}");
        assert_eq!(fs::read_dir(out2.path()).unwrap().count(), 0);

        fs::write(
            source.path().join("model_capabilities.csv"),
            CAPABILITIES_CSV,
        )
        .unwrap();
        generate(source.path(), out.path(), false).expect("the retry is not refused");
        assert!(out.path().join("router.yml").exists());
    }

    /// A shape error on a `keys` entry must not put the value on the terminal:
    /// serde_yaml quotes it verbatim, and in `providers.yaml` it is a live key.
    #[test]
    fn a_malformed_providers_yaml_never_quotes_the_offending_value() {
        let dir = tempfile::tempdir().expect("a throwaway directory");
        let path = dir.path().join("providers.yaml");
        fs::write(
            &path,
            "providers:\n  - name: zai\n    wire: openai\n    base_url: https://api.z.ai\n    keys: sk-live-AAAABBBBCCCC\n",
        )
        .unwrap();

        let err = read_providers(&path).expect_err("a scalar where a sequence belongs is an error");
        let text = format!("{err:?}");
        assert!(
            !text.contains("sk-live"),
            "the error text leaked key material: {text}"
        );
        assert!(text.contains("line 5"), "no location reported: {text}");
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
        assert_eq!(keys_env_name("302ai"), "_302AI_KEYS");
    }

    /// A generated name must survive the router's own `.env` parser: `dotenvy`
    /// drops a line whose identifier starts with a digit.
    #[test]
    fn env_variable_names_parse_back_out_of_a_dotenv_file() {
        let name = keys_env_name("302ai");
        let parsed = llmrouter::config::parse_env_file(&format!("{name}=aaa,bbb\n"));
        assert_eq!(parsed.get(&name).map(Vec::len), Some(2));
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
        // known_models reaches providers.yml only for a provider with no
        // model-listing endpoint. Cloudflare has none, so it keeps its list.
        assert_eq!(providers[1].models, vec!["@cf/meta/llama".to_string()]);
        // zai lists its models, so it gets no fallback: a 404 or 405 on its
        // keyed /models probe has to surface as unhealthy, not be masked by a
        // one-model catalogue.
        assert!(providers[0].models.is_empty());
        assert!(providers[2].models.is_empty());
        // One that sets it keeps what it set, and the wire name survives.
        assert_eq!(providers[2].models_path, "/v1/models");
        assert_eq!(providers[2].wire, WireName::Anthropic);
    }

    #[test]
    fn a_base_url_missing_its_version_segment_is_corrected() {
        // groq and mistral arrive without the `/v1`, so the keyed /models
        // probe 404s and the provider yields no channels at all.
        assert_eq!(
            fix_base_url("https://api.groq.com/openai/"),
            "https://api.groq.com/openai/v1"
        );
        assert_eq!(
            fix_base_url("https://api.mistral.ai"),
            "https://api.mistral.ai/v1"
        );
        // One that already carries a version segment is left alone, whatever
        // that segment happens to be.
        assert_eq!(
            fix_base_url("https://api.z.ai/api/coding/paas/v4/"),
            "https://api.z.ai/api/coding/paas/v4"
        );
        assert_eq!(
            fix_base_url("https://api.groq.com/openai/v1"),
            "https://api.groq.com/openai/v1"
        );
    }

    #[test]
    fn the_corrected_base_url_reaches_providers_yml() {
        let source = tempfile::tempdir().unwrap();
        fs::write(
            source.path().join("providers.yaml"),
            "providers:\n  - name: groq\n    wire: openai\n    \
             base_url: https://api.groq.com/openai\n    keys: [one]\n",
        )
        .unwrap();
        fs::write(source.path().join("models.csv"), MODELS_CSV).unwrap();
        fs::write(
            source.path().join("model_capabilities.csv"),
            CAPABILITIES_CSV,
        )
        .unwrap();
        let out = tempfile::tempdir().unwrap();
        generate(source.path(), out.path(), false).expect("generates");

        let config = llmrouter::Config::load(out.path()).expect("the router loads it");
        assert_eq!(
            config.providers[0].base_url,
            "https://api.groq.com/openai/v1"
        );
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
                0,
                false
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

    /// The live run dropped hades-local (http://hades-ubuntu.arpa:8021) and
    /// chatjimmy-local (http://localhost:4100) at the keyless reachability
    /// probe. Both are LAN addresses that only answer on the host they run on,
    /// so the summary names them instead of leaving an operator to read the
    /// probe failure as a broken config.
    #[test]
    fn lan_only_providers_are_flagged_in_the_summary() {
        for url in [
            "http://hades-ubuntu.arpa:8021",
            "http://localhost:4100",
            "http://127.0.0.1:8080",
            "http://192.168.1.10/v1",
            "http://hades-ubuntu:8021",
            "http://printer.local",
            "http://[::1]:8021",
            "http://[fd7a:115c:a1e0::1]:8021",
            "http://[fe80::1]:8021",
            "http://100.100.64.3:8021",
        ] {
            assert!(is_lan_only(url), "{url} should be LAN only");
        }
        for url in [
            "https://api.z.ai/api/coding/paas/v4",
            "https://api.anthropic.com",
            "http://203.0.113.7:8021",
            "http://[2001:db8::1]:8021",
            "http://100.63.0.1:8021",
            "not a url",
        ] {
            assert!(!is_lan_only(url), "{url} should not be LAN only");
        }

        let report = Report {
            providers: vec![
                ("zai".to_string(), "ZAI_KEYS".to_string(), 2, false),
                (
                    "hades-local".to_string(),
                    "HADES_LOCAL_KEYS".to_string(),
                    1,
                    true,
                ),
            ],
            ..Default::default()
        };
        let rendered = report.render(Path::new("/tmp/config"));
        assert!(rendered.contains("LAN or loopback only: hades-local"));
        assert!(rendered.contains("its own host"));
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

    #[cfg(unix)]
    #[test]
    fn a_forced_rewrite_narrows_a_world_readable_env() {
        use std::os::unix::fs::PermissionsExt;
        let source = source_dir();
        let out = tempfile::tempdir().unwrap();
        let env_path = out.path().join(".env");
        // An operator made it by hand under umask 022, so it is world readable.
        fs::write(&env_path, "HAND_EDITED_KEYS=keep-me\n").unwrap();
        fs::set_permissions(&env_path, fs::Permissions::from_mode(0o644)).unwrap();

        generate(source.path(), out.path(), true).expect("--force replaces it");

        let mode = fs::metadata(&env_path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "forced rewrite must narrow the mode");
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
    fn a_renamed_score_column_is_an_error_not_an_empty_ranking() {
        let source = source_dir();
        let renamed = MODELS_CSV.replacen("intelligence_rank", "rank", 1);
        fs::write(source.path().join("models.csv"), renamed).unwrap();
        let out = tempfile::tempdir().unwrap();

        let err = generate(source.path(), out.path(), false).expect_err("refuses");
        assert!(err.to_string().contains("intelligence_rank"), "{err}");
        assert_eq!(fs::read_dir(out.path()).unwrap().count(), 0);
    }

    #[test]
    fn a_models_csv_where_nothing_ranks_is_an_error() {
        let source = source_dir();
        fs::write(
            source.path().join("models.csv"),
            "slug,intelligence,intelligence_rank\nno-score-model,,\n",
        )
        .unwrap();
        let out = tempfile::tempdir().unwrap();

        let err = generate(source.path(), out.path(), false).expect_err("refuses");
        assert!(err.to_string().contains("no row"), "{err}");
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
    fn hand_edited_models_and_router_survive_a_rerun() {
        let source = source_dir();
        let out = tempfile::tempdir().unwrap();
        let models = "- provider: zai\n  model_id: glm-5.3\n  ranks_as: glm-5-3\n";
        let router = "race_width: 3\n";
        fs::write(out.path().join("models.yml"), models).unwrap();
        fs::write(out.path().join("router.yml"), router).unwrap();

        // --force, the case that regenerates the .env, must still leave these
        // two alone.
        let report = generate(source.path(), out.path(), true).expect("regenerates");

        assert_eq!(
            fs::read_to_string(out.path().join("models.yml")).unwrap(),
            models
        );
        assert_eq!(
            fs::read_to_string(out.path().join("router.yml")).unwrap(),
            router
        );
        assert_eq!(report.kept, vec!["models.yml", "router.yml"]);
        assert!(report.render(out.path()).contains("Left as they were"));

        // A directory without them still gets both templates.
        let fresh = tempfile::tempdir().unwrap();
        let report = generate(source.path(), fresh.path(), false).expect("writes both");
        assert!(report.kept.is_empty());
        assert!(fresh.path().join("models.yml").exists());
        assert!(fresh.path().join("router.yml").exists());
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

    /// `--out .` with the default `--source .` must not rewrite the tracked
    /// source snapshot with the Cloudflare alias rows, nor write anything else.
    #[test]
    fn out_equal_to_source_is_refused_and_the_source_csv_is_untouched() {
        let source = source_dir();
        // A different spelling of the same directory, so only canonicalising
        // catches it.
        let out = source.path().join(".");
        let err = generate(source.path(), &out, false).expect_err("refuses");
        assert!(err.to_string().contains("model_capabilities.csv"), "{err}");
        assert_eq!(
            fs::read_to_string(source.path().join("model_capabilities.csv")).unwrap(),
            CAPABILITIES_CSV
        );
        assert!(!source.path().join(".env").exists());
    }

    /// An unmounted share looks like a missing parent. The run must refuse
    /// rather than build the tree on the local disk and put keys there.
    #[test]
    fn a_missing_parent_is_refused_and_nothing_is_created() {
        let source = source_dir();
        let parent = tempfile::tempdir().unwrap();
        let out = parent.path().join("share").join("config");
        let err = generate(source.path(), &out, false).expect_err("refuses");
        assert!(err.to_string().contains("mounted"), "{err}");
        assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    /// A failed run must not leave a freshly made output directory behind.
    #[test]
    fn a_failed_run_does_not_create_the_output_directory() {
        let source = tempfile::tempdir().unwrap();
        fs::write(source.path().join("providers.yaml"), PROVIDERS_YAML).unwrap();
        let parent = tempfile::tempdir().unwrap();
        let out = parent.path().join("config");
        generate(source.path(), &out, false).expect_err("no models.csv");
        assert!(!out.exists());
    }

    #[test]
    fn the_output_directory_is_created_and_holds_all_six_files() {
        let source = source_dir();
        let parent = tempfile::tempdir().unwrap();
        let out = parent.path().join("config");
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
        // Three source rows, plus the Workers AI row aliased onto the one
        // cloudflare account in the fixture.
        assert_eq!(report.capability_rows, 4);
        // The source rows go across byte for byte, with the aliased ones
        // appended after them.
        assert!(fs::read_to_string(out.join("model_capabilities.csv"))
            .unwrap()
            .starts_with(CAPABILITIES_CSV));
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

    /// The capabilities export names Cloudflare `cloudflare-workers-ai`, while
    /// `providers.yaml` names each account `cloudflare-<account id>`. Without
    /// the alias the join finds nothing, the channel takes the 8,192 token
    /// default and sits out every prompt longer than that.
    #[test]
    fn cloudflare_accounts_join_the_workers_ai_capability_rows() {
        let source = source_dir();
        let out = tempfile::tempdir().unwrap();
        generate(source.path(), out.path(), false).expect("generates");

        let config = llmrouter::Config::load(out.path()).expect("the router loads it");
        let capability = config
            .capability("cloudflare-b4b050cc", "@cf/meta/llama")
            .expect("the Workers AI row reaches the account that serves it");
        assert_eq!(capability.limit_context, Some(128_000));
        assert_eq!(capability.limit_output, Some(4096));
        assert_eq!(capability.tool_call, Some(true));
        // The row it was copied from stays where it was.
        assert!(config
            .capability("cloudflare-workers-ai", "@cf/meta/llama")
            .is_some());
        // A provider with no such prefix gets nothing it did not already have.
        assert!(config.capability("zai", "@cf/meta/llama").is_none());
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
        assert_eq!(parsed.hedge_delay_ms, 1);
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

    /// The generator must read the same variable the router reads, or off the
    /// deploy host the two end up on different directories. Asserted against
    /// the parser rather than by setting the variable, because the environment
    /// is process wide and the other tests run beside this one.
    #[test]
    fn the_output_directory_reads_the_routers_own_variable() {
        use clap::CommandFactory;
        let env = Args::command()
            .get_arguments()
            .find(|a| a.get_id() == "out")
            .expect("an --out argument")
            .get_env()
            .expect("--out reads an environment variable")
            .to_owned();
        assert_eq!(env, "LLMROUTER_CONFIG_DIR");
    }

    #[test]
    fn the_arguments_default_to_the_deploy_path() {
        let args = Args::parse_from(["llmrouter-make-config"]);
        if std::env::var_os("LLMROUTER_CONFIG_DIR").is_none() {
            assert_eq!(args.out, PathBuf::from(DEFAULT_OUT));
        }
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
