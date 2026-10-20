//! Smoke tier: the built image under docker compose.
//!
//! See tests/README.md. This tier proves the container starts and serves, and
//! nothing else: bring the stack up, check the three read-only endpoints
//! answer with the right shape, bring it down again. Business logic belongs in
//! the contract and integration tiers, which run in-process and contribute
//! coverage.
//!
//! Docker is not available on every development machine, so this skips with a
//! message rather than failing when there is no working daemon. CI runs it for
//! real on an ubuntu runner.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

/// The repo root, which is where the compose files live.
const ROOT: &str = env!("CARGO_MANIFEST_DIR");

/// The router is a release build inside the image, so the first `up` can take
/// a while on a cold runner.
const READY_TIMEOUT: Duration = Duration::from_secs(180);

/// `docker-compose.ci.yml` publishes a kernel-chosen port on the loopback
/// address, so the base URL is read back from compose after `up` rather than
/// pinned here. Pinning it raced anything else holding the fixed port.
///
/// A runner whose docker cannot publish ports (no iptables in the container)
/// returns an address that refuses every connection while the router inside
/// answers fine, so the container's own bridge IP is offered as a second
/// candidate: it needs no port publishing at all, only a route to the docker
/// bridge, which the host always has.
fn base_urls() -> Result<Vec<String>, String> {
    let printed = compose_command(&["port", "llmrouter", "8080"])
        .output()
        .map_err(|error| format!("docker compose port failed: {error}"))?;
    if !printed.status.success() {
        return Err(format!(
            "docker compose port failed: {}",
            String::from_utf8_lossy(&printed.stderr).trim()
        ));
    }
    let printed = String::from_utf8_lossy(&printed.stdout).trim().to_owned();
    // Compose prints `address:port`, and the address may be `0.0.0.0` or an
    // IPv6 form. Only the port is wanted, and the publish is loopback only.
    let port = printed
        .rsplit_once(':')
        .map(|(_, port)| port)
        .filter(|port| port.parse::<u16>().is_ok())
        .ok_or_else(|| format!("docker compose port printed {printed:?}"))?;
    let mut bases = vec![format!("http://127.0.0.1:{port}")];
    let id = compose_command(&["ps", "-q", "llmrouter"])
        .output()
        .map_err(|error| format!("docker compose ps failed: {error}"))?;
    let id = String::from_utf8_lossy(&id.stdout).trim().to_owned();
    if let Ok(inspect) = compose_command(&[
        "inspect",
        "-f",
        "{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
        &id,
    ])
    .output()
    {
        let ip = String::from_utf8_lossy(&inspect.stdout).trim().to_owned();
        if !ip.is_empty() {
            bases.push(format!("http://{ip}:8080"));
        }
    }
    Ok(bases)
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// The Compose project name for this checkout.
///
/// Compose's default is the basename of the directory holding the first
/// compose file, lowercased with non-alphanumerics stripped, not the full
/// path. The project name decides the image name, the container name and the
/// named volume, so two clones both called `llmrouter` share all three: one
/// run's `up --build` recreates the other's container mid-test and its
/// `down -v` deletes the other's volume. Keying on the full path keeps the
/// runs apart. ci.sh exports the same variable (keyed on the same checkout
/// path) so its cleanup trap takes down the stack this test brought up.
fn project_name(dir: &str) -> String {
    std::env::var("COMPOSE_PROJECT_NAME").unwrap_or_else(|_| {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        dir.hash(&mut hasher);
        format!("llmrouter-smoke-{:x}", hasher.finish())
    })
}

/// `docker compose -f docker-compose.yml -f docker-compose.ci.yml <args>`,
/// pinned to this checkout's own project.
fn compose_command(args: &[&str]) -> Command {
    let mut command = Command::new("docker");
    command
        .current_dir(ROOT)
        .env("COMPOSE_PROJECT_NAME", project_name(ROOT))
        .args([
            "compose",
            "-f",
            "docker-compose.yml",
            "-f",
            "docker-compose.ci.yml",
        ])
        .args(args);
    command
}

fn compose(args: &[&str]) -> std::process::ExitStatus {
    compose_command(args).status().expect("docker compose runs")
}

/// One stack per run, so all three endpoints are checked inside a single
/// `up`/`down` cycle. Splitting them into separate `#[test]`s would race for
/// the published port.
#[tokio::test]
async fn the_built_image_starts_and_serves_its_read_only_endpoints() {
    if !docker_available() {
        // A skip is for a developer laptop. On the runner, where this tier is
        // the only thing that exercises the image, a missing daemon means the
        // job proved nothing, so it fails rather than passing green.
        assert!(
            std::env::var_os("CI").is_none(),
            "no working docker daemon, and CI is set: the smoke tier cannot \
             pass without one"
        );
        eprintln!(
            "SKIPPED smoke tier: no working docker daemon on this machine. \
             The smoke tests run in GitHub Actions on an ubuntu runner."
        );
        return;
    }

    // Always build through the compose file's own build section, so a broken
    // context, dockerfile, target or args fails here, locally and in CI alike.
    let built = compose(&["up", "-d", "--build"]).success();
    let result = if built {
        match base_urls() {
            Ok(bases) => check_endpoints(&reqwest::Client::new(), &bases[0], &bases).await,
            Err(reason) => Err(reason),
        }
    } else {
        Err("docker compose up failed".to_owned())
    };

    teardown(
        &result,
        || {
            let _ = compose(&["logs", "--no-color"]);
        },
        || {
            let _ = compose(&["down", "-v"]);
        },
    );

    if let Err(reason) = result {
        panic!("{reason}");
    }
}

/// Down regardless, so a failure does not leave the stack running or the named
/// volume behind for the next run to inherit. On the failure path the logs are
/// dumped first: `down -v` destroys the containers, so anything read after it
/// comes back empty, and the logs are the only diagnostic a CI run leaves.
fn teardown(result: &Result<(), String>, mut logs: impl FnMut(), mut down: impl FnMut()) {
    if result.is_err() {
        logs();
    }
    down();
}

#[test]
fn teardown_dumps_the_logs_before_it_destroys_the_stack() {
    let order = std::cell::RefCell::new(Vec::new());
    teardown(
        &Err("boom".to_owned()),
        || order.borrow_mut().push("logs"),
        || order.borrow_mut().push("down"),
    );
    assert_eq!(order.into_inner(), ["logs", "down"]);

    let order = std::cell::RefCell::new(Vec::new());
    teardown(
        &Ok(()),
        || order.borrow_mut().push("logs"),
        || order.borrow_mut().push("down"),
    );
    assert_eq!(order.into_inner(), ["down"]);
}

async fn check_endpoints(
    client: &reqwest::Client,
    base: &str,
    bases: &[String],
) -> Result<(), String> {
    wait_for_healthz(client, bases).await?;
    check_metrics(client, base).await?;
    check_models(client, base).await
}

/// The container is up when `/healthz` answers 200 with the status document
/// the server builds, not merely when the port accepts a connection.
async fn wait_for_healthz(client: &reqwest::Client, bases: &[String]) -> Result<(), String> {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        for base in bases {
            let attempt = client.get(format!("{base}/healthz")).send().await;
            match attempt {
                Ok(reply) if reply.status().is_success() => {
                    let body: Value = reply
                        .json()
                        .await
                        .map_err(|error| format!("/healthz is not JSON: {error}"))?;
                    if body["status"] != "ok" {
                        return Err(format!("/healthz status field was {}", body["status"]));
                    }
                    if !body["channels"]["total"].is_u64() {
                        return Err(format!("/healthz had no channel count: {body}"));
                    }
                    return Ok(());
                }
                other => {
                    if Instant::now() >= deadline {
                        return Err(format!("/healthz never came up: {other:?}"));
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Prometheus text, not JSON and not an empty body. Every metric is registered
/// at zero on boot, so the names are there before any request has been served.
async fn check_metrics(client: &reqwest::Client, base: &str) -> Result<(), String> {
    let reply = get(client, base, "/metrics").await?;
    let content_type = reply
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    if !content_type.starts_with("text/plain") {
        return Err(format!("/metrics content type was {content_type:?}"));
    }

    let body = reply
        .text()
        .await
        .map_err(|error| format!("/metrics body unreadable: {error}"))?;
    for expected in ["# HELP router_requests_total", "router_rss_bytes"] {
        if !body.contains(expected) {
            return Err(format!("/metrics did not mention {expected}: {body}"));
        }
    }
    Ok(())
}

/// Valid JSON in the OpenAI list shape. The fixture config has no providers,
/// so the rows are expected to be empty: the shape is the point, not the
/// contents.
async fn check_models(client: &reqwest::Client, base: &str) -> Result<(), String> {
    let body: Value = get(client, base, "/v1/models")
        .await?
        .json()
        .await
        .map_err(|error| format!("/v1/models is not JSON: {error}"))?;

    if body["object"] != "list" {
        return Err(format!("/v1/models object field was {}", body["object"]));
    }
    if !body["data"].is_array() {
        return Err(format!("/v1/models data was not an array: {body}"));
    }
    Ok(())
}

async fn get(
    client: &reqwest::Client,
    base: &str,
    path: &str,
) -> Result<reqwest::Response, String> {
    let reply = client
        .get(format!("{base}{path}"))
        .send()
        .await
        .map_err(|error| format!("GET {path} failed: {error}"))?;
    if !reply.status().is_success() {
        return Err(format!("GET {path} returned {}", reply.status()));
    }
    Ok(reply)
}

/// The overlay must *replace* the base file's published ports, not add to
/// them. Compose concatenates `ports` across files, so without the `!override`
/// tag the service publishes 8080 on 0.0.0.0 and then fails to bind
/// 127.0.0.1:8080, and `up` never succeeds. Checked by reading the file rather
/// than by running compose, so it holds on a machine with no docker.
#[test]
fn the_ci_overlay_replaces_the_published_ports_rather_than_adding_to_them() {
    let overlay: serde_yaml::Value = serde_yaml::from_str(
        &std::fs::read_to_string(format!("{ROOT}/docker-compose.ci.yml")).unwrap(),
    )
    .expect("the CI overlay is YAML");
    let ports = &overlay["services"]["llmrouter"]["ports"];

    let serde_yaml::Value::Tagged(tagged) = ports else {
        panic!("docker-compose.ci.yml must tag `ports` with !override or !reset, found {ports:?}");
    };
    assert!(
        matches!(tagged.tag.to_string().as_str(), "!override" | "!reset"),
        "unexpected tag on `ports`: {}",
        tagged.tag
    );
}

/// Two checkouts running the smoke tier at once must not share a stack. The
/// base compose file is what the NAS runs, and the NAS only pulls: it must
/// name the published image and carry no `build:` key. The smoke tier's image
/// therefore comes from the overlay, which builds from the checkout and tags
/// it with the per-checkout project name — a fixed tag such as
/// llmrouter-smoke-acme would be global to the daemon, so two runs could
/// retag one another's binary between build and container create. The project
/// name is keyed on the checkout's full path, because Compose's own default
/// is only the directory basename and two clones of this repo almost always
/// share one. Read from the files so it holds on a machine with no daemon.
#[test]
fn each_checkout_gets_its_own_image_container_and_volume() {
    let base: serde_yaml::Value = serde_yaml::from_str(
        &std::fs::read_to_string(format!("{ROOT}/docker-compose.yml")).unwrap(),
    )
    .expect("the base compose file is YAML");
    let service = &base["services"]["llmrouter"];
    assert!(
        service["build"].is_null(),
        "docker-compose.yml must not set `build:`, found {:?}: the NAS cannot build images, it pulls",
        service["build"]
    );
    assert!(
        service["image"]
            .as_str()
            .is_some_and(|image| image.starts_with("ghcr.io/")),
        "docker-compose.yml must pull the published image from ghcr, found {:?}",
        service["image"]
    );

    let overlay: serde_yaml::Value = serde_yaml::from_str(
        &std::fs::read_to_string(format!("{ROOT}/docker-compose.ci.yml")).unwrap(),
    )
    .expect("the CI overlay is YAML");
    let smoke = &overlay["services"]["llmrouter"];
    let image = smoke["image"].as_str().unwrap_or_default();
    assert!(
        image.contains("${COMPOSE_PROJECT_NAME}"),
        "the overlay must key the smoke image on COMPOSE_PROJECT_NAME, found {image:?}, or two checkouts share one daemon-global tag"
    );
    assert!(
        !smoke["build"].is_null(),
        "the overlay must carry the `build:` key, or the smoke tier pulls the published image instead of testing this checkout"
    );

    // Same basename, different path: the names must still differ, or the two
    // runs fight over one container and one volume. Only meaningful with the
    // variable unset, which is how `cargo test --test smoke` runs by hand;
    // under ci.sh it is exported and the script keys it on $PWD itself.
    if std::env::var_os("COMPOSE_PROJECT_NAME").is_none() {
        assert_ne!(
            project_name("/one/llmrouter"),
            project_name("/two/llmrouter"),
            "the project name must be keyed on the whole checkout path"
        );
    }

    // ci.sh runs `compose down -v` from its cleanup trap, outside this
    // process, so it has to set the same project name rather than inherit
    // Compose's basename default.
    let ci_sh = std::fs::read_to_string(format!("{ROOT}/ci.sh")).expect("ci.sh is readable");
    assert!(
        ci_sh.contains("export COMPOSE_PROJECT_NAME="),
        "ci.sh must export COMPOSE_PROJECT_NAME, or its cleanup trap tears down whichever checkout's stack Compose's default happens to name"
    );
}

/// The build context must carry the crate sources and nothing else. The build
/// stage does `COPY . .` from the repo root, which at runtime also holds
/// `config/.env` with the live provider keys and `data/cache.redb` with the
/// cached prompts and answers. The final stage copies only the binary, but the
/// build stage is still a real addressable layer in the local image store and
/// in any exported BuildKit cache, so anything sent to it has leaked.
///
/// Read from the file rather than run through docker, so it holds on a machine
/// with no daemon. An allow list is pinned here on purpose: a new runtime file
/// in the repo root is then excluded by default, rather than by somebody
/// remembering to add a rule for it.
#[test]
fn the_build_context_carries_the_crate_sources_and_nothing_else() {
    // Tracked, not merely present. `actions/checkout` materialises tracked
    // content only, so an unstaged .dockerignore passes on the maintainer's
    // machine and leaves the Actions runner sending the whole repo root.
    let tracked = std::process::Command::new("git")
        .args(["ls-files", "--error-unmatch", ".dockerignore"])
        .current_dir(ROOT)
        .output()
        .expect("git must be on PATH");
    assert!(
        tracked.status.success(),
        ".dockerignore must be tracked by git, or the Actions checkout has none and `COPY . .` sends the whole repo root"
    );

    let rules = std::fs::read_to_string(format!("{ROOT}/.dockerignore"))
        .expect(".dockerignore must exist, or `COPY . .` bakes the keys into the build layer");
    let rules: Vec<&str> = rules
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();

    let (first, rest) = rules.split_first().expect(".dockerignore must have rules");
    assert_eq!(*first, "*", ".dockerignore must deny everything first");

    let allowed: Vec<&str> = rest
        .iter()
        .map(|line| {
            line.strip_prefix('!')
                .unwrap_or_else(|| panic!("only re-inclusions may follow `*`, found {line:?}"))
        })
        .collect();
    assert_eq!(allowed, ["src", "static", "Cargo.toml", "Cargo.lock"]);
}
