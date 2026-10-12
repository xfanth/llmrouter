//! build_providers.py writes every live key into one file, so that file must
//! come out owner-only whatever the umask, and a copy an older run left
//! world-readable must be narrowed, not reused at its old mode.

use std::os::unix::fs::PermissionsExt;
use std::process::Command;

#[test]
fn build_providers_writes_the_key_file_owner_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir.path().join("channels.csv");
    let out = dir.path().join("providers.yaml");
    std::fs::write(
        &src,
        "name,base_url,priority,key,model_mapping\n\
         groq,https://api.groq.com/openai/chat/completions,5,placeholder-not-a-key,\n",
    )
    .expect("fixture written");
    // The state on the share today: a previous run left it at 0644.
    std::fs::write(&out, "stale\n").expect("stale output written");
    std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o644)).expect("chmod 644");

    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("build_providers.py");
    // Run from the tempdir so the script's optional freellmapi-keys.csv is absent.
    let run = Command::new("python3")
        .arg(&script)
        .arg(&src)
        .arg(&out)
        .current_dir(dir.path())
        .output()
        .expect("python3 runs");
    assert!(run.status.success(), "build_providers.py failed");

    let mode = std::fs::metadata(&out)
        .expect("output exists")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "providers.yaml is {mode:o}, must be 600");
    let body = std::fs::read_to_string(&out).expect("output readable");
    assert!(
        body.contains("placeholder-not-a-key"),
        "the new content replaced the stale file"
    );
}

/// Both inputs hold the same plaintext keys as the output, so a run must
/// leave them owner-only too, not read them at 0755 and report success.
#[test]
fn build_providers_narrows_its_key_inputs_to_owner_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir.path().join("channels.csv");
    let extra = dir.path().join("freellmapi-keys.csv");
    let out = dir.path().join("providers.yaml");
    std::fs::write(
        &src,
        "name,base_url,priority,key,model_mapping\n\
         groq,https://api.groq.com/openai/chat/completions,5,placeholder-not-a-key,\n",
    )
    .expect("fixture written");
    std::fs::write(&extra, "platform,key,label\ngroq,placeholder-extra,x\n")
        .expect("extra fixture written");
    for f in [&src, &extra] {
        std::fs::set_permissions(f, std::fs::Permissions::from_mode(0o755)).expect("chmod 755");
    }

    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("build_providers.py");
    let run = Command::new("python3")
        .arg(&script)
        .arg(&src)
        .arg(&out)
        .current_dir(dir.path())
        .output()
        .expect("python3 runs");
    assert!(run.status.success(), "build_providers.py failed");

    for f in [&src, &extra] {
        let mode = std::fs::metadata(f)
            .expect("input exists")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "{} is {mode:o}, must be 600", f.display());
    }
}
