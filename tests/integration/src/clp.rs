//! Real CLP engine fixture. Tests fail (rather than skip) if the engine is unavailable.
use std::{path::PathBuf, process::Command, sync::OnceLock};

const CLP_IMAGE: &str = concat!(
    "ghcr.io/y-scope/clp/clp-package:0.14.1@sha256:",
    "f04d30e9243b2af0e549f2e81e547c0b9640d4e9f02c00aed34771987115671a"
);

pub fn binary() -> PathBuf {
    if let Some(path) = std::env::var_os("METALOG_TEST_CLP_BINARY") {
        return path.into();
    }
    static CLP: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    CLP.get_or_init(|| {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("clp-s");
        let output = Command::new("docker")
            .args(["create", "--entrypoint", "/bin/true", CLP_IMAGE])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "prepare CLP with docker pull ghcr.io/y-scope/clp/clp-package:0.14.1 or set \
             METALOG_TEST_CLP_BINARY: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let id = String::from_utf8(output.stdout).unwrap();
        let copied = Command::new("docker")
            .args(["cp", &format!("{}:/opt/clp/bin/clp-s", id.trim())])
            .arg(&path)
            .status()
            .unwrap();
        let _ = Command::new("docker").args(["rm", id.trim()]).output();
        assert!(copied.success());
        (temp, path)
    })
    .1
    .clone()
}
