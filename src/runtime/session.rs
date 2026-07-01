use std::{
    fmt,
    hash::{Hash, Hasher},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SessionId(String);

impl SessionId {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();

        if !is_valid_id(&value) {
            return Err(format!("invalid session id `{value}`"));
        }

        Ok(Self(value))
    }

    pub fn for_scope(scope: &SessionScope) -> Self {
        let mut hasher = StableHasher::default();
        scope.platform.hash(&mut hasher);
        "\0".hash(&mut hasher);
        scope.scope.hash(&mut hasher);

        Self(format!("session_{:016x}", hasher.finish()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionScope {
    pub platform: String,
    pub scope: String,
}

impl SessionScope {
    pub fn new(platform: impl Into<String>, scope: impl Into<String>) -> Result<Self, String> {
        let platform = platform.into();
        let scope = scope.into();

        if platform.trim().is_empty() {
            return Err("session platform must not be empty".to_owned());
        }

        if scope.trim().is_empty() {
            return Err("session scope must not be empty".to_owned());
        }

        Ok(Self { platform, scope })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub scope: SessionScope,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
}

impl Session {
    pub fn new(scope: SessionScope) -> Self {
        let now = unix_seconds_now();

        Self {
            id: SessionId::for_scope(&scope),
            scope,
            created_at_unix: now,
            updated_at_unix: now,
        }
    }

    pub fn touch(&mut self) {
        self.updated_at_unix = unix_seconds_now();
    }
}

fn is_valid_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Default)]
struct StableHasher {
    state: u64,
}

impl Hasher for StableHasher {
    fn write(&mut self, bytes: &[u8]) {
        let mut state = if self.state == 0 {
            0xcbf29ce484222325
        } else {
            self.state
        };

        for byte in bytes {
            state ^= u64::from(*byte);
            state = state.wrapping_mul(0x100000001b3);
        }

        self.state = state;
    }

    fn finish(&self) -> u64 {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::{Session, SessionId, SessionScope};

    #[test]
    fn session_id_is_stable_for_scope() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let first = Session::new(scope.clone());
        let second = Session::new(scope);

        assert_eq!(first.id, second.id);
        assert!(first.id.as_str().starts_with("session_"));
    }

    #[test]
    fn rejects_invalid_session_ids() {
        assert!(SessionId::new("").is_err());
        assert!(SessionId::new("bad id").is_err());
        assert!(SessionId::new("session_ok-1").is_ok());
    }
}
