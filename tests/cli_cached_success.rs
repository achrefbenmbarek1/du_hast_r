use std::fs;
use std::process::Command;

use sha2::Digest;

#[test]
fn cli_returns_success_for_valid_cached_request() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let cache_dir = temp_dir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache dir");

    let body = b"integration test tarball";
    let checksum = hex::encode(sha2::Sha256::digest(body));
    let url = "https://mirror.example.org/src/contrib/BiocGenerics_0.50.0.tar.gz";
    let artifact_name = "BiocGenerics_0.50.0.tar.gz";

    let artifact_path = async_dependency_installer_for_r::cached_artifact_path(
        &cache_dir,
        url,
        &async_dependency_installer_for_r::Checksum {
            algorithm: "sha256".to_string(),
            value: checksum.clone(),
        },
        Some(artifact_name),
    );
    fs::write(&artifact_path, body).expect("seed cache");

    let request_path = temp_dir.path().join("request.json");
    let request = serde_json::json!({
        "cache_dir": cache_dir,
        "concurrency": 4,
        "packages": [{
            "package": "BiocGenerics",
            "version": "0.50.0",
            "urls": [url],
            "checksum": {
                "algorithm": "sha256",
                "value": checksum
            },
            "artifact_name": artifact_name
        }]
    });
    fs::write(
        &request_path,
        serde_json::to_vec_pretty(&request).expect("serialize request"),
    )
    .expect("write request");

    let output = Command::new(env!("CARGO_BIN_EXE_async_dependency_installer_for_R"))
        .arg(&request_path)
        .output()
        .expect("run binary");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let response: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse JSON response");
    assert_eq!(response["results"][0]["status"]["kind"], "success");
    assert_eq!(response["results"][0]["status"]["cached"], true);
}
