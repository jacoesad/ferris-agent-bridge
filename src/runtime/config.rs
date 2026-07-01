use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::state::{read_json, write_json_atomic};

pub const CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeConfig {
    pub version: u32,
    pub profile: String,
    pub runtime: RuntimeConfig,
    pub secret: Option<SecretInput>,
}

impl BridgeConfig {
    pub fn new(profile: impl Into<String>) -> Self {
        Self {
            version: CONFIG_VERSION,
            profile: profile.into(),
            runtime: RuntimeConfig::default(),
            secret: None,
        }
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let config: Self = read_json(path)?;
        config.validate()?;
        Ok(config)
    }

    pub fn save(&self, path: &Path) -> Result<(), String> {
        self.validate()?;
        write_json_atomic(path, self)
            .map_err(|err| format!("failed to save config {}: {err}", path.display()))
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != CONFIG_VERSION {
            return Err(format!(
                "unsupported config version {}; expected {}",
                self.version, CONFIG_VERSION
            ));
        }

        if self.profile.trim().is_empty() {
            return Err("config profile must not be empty".to_owned());
        }

        self.runtime.validate()?;

        if let Some(secret) = &self.secret {
            secret.validate()?;
        }

        Ok(())
    }
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self::new("default")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub state_file: PathBuf,
    pub log_file: PathBuf,
}

impl RuntimeConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.state_file.as_os_str().is_empty() {
            return Err("runtime state_file must not be empty".to_owned());
        }

        if self.log_file.as_os_str().is_empty() {
            return Err("runtime log_file must not be empty".to_owned());
        }

        Ok(())
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            state_file: PathBuf::from("runtime.state.json"),
            log_file: PathBuf::from("runtime.log"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum SecretInput {
    Env { name: String },
    File { path: PathBuf },
}

impl SecretInput {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Env { name } if name.trim().is_empty() => {
                Err("secret env name must not be empty".to_owned())
            }
            Self::File { path } if path.as_os_str().is_empty() => {
                Err("secret file path must not be empty".to_owned())
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::{BridgeConfig, RuntimeConfig, SecretInput};

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn config_round_trips_as_json() {
        let dir = test_path("config-round-trip");
        let path = dir.join("config.json");
        let mut config = BridgeConfig::new("work");
        config.runtime = RuntimeConfig {
            state_file: PathBuf::from("state.json"),
            log_file: PathBuf::from("runtime.log"),
        };
        config.secret = Some(SecretInput::Env {
            name: "FERRIS_TOKEN".to_owned(),
        });

        config.save(&path).expect("config should save");
        let loaded = BridgeConfig::load(&path).expect("config should load");

        assert_eq!(loaded, config);
    }

    #[test]
    fn config_validation_rejects_empty_profile() {
        let mut config = BridgeConfig::default();
        config.profile.clear();

        let err = config.validate().expect_err("profile should be required");

        assert!(err.contains("profile"));
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ferris-agent-bridge-{name}-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("test dir should exist");
        path
    }
}
