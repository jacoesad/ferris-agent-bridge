use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Deserializer, Serialize, de};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
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
        Self(format!(
            "session_v1_{}_{}",
            encode_canonical_component(&scope.platform),
            encode_canonical_component(&scope.scope)
        ))
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

impl<'de> Deserialize<'de> for SessionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionScope {
    platform: String,
    scope: String,
}

impl SessionScope {
    pub fn new(platform: impl Into<String>, scope: impl Into<String>) -> Result<Self, String> {
        let scope = Self {
            platform: platform.into(),
            scope: scope.into(),
        };

        scope.validate()?;
        Ok(scope)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.platform.trim().is_empty() {
            return Err("session platform must not be empty".to_owned());
        }

        if self.scope.trim().is_empty() {
            return Err("session scope must not be empty".to_owned());
        }

        Ok(())
    }

    pub fn platform(&self) -> &str {
        &self.platform
    }

    pub fn scope(&self) -> &str {
        &self.scope
    }
}

impl<'de> Deserialize<'de> for SessionScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SessionScopeWire {
            platform: String,
            scope: String,
        }

        let wire = SessionScopeWire::deserialize(deserializer)?;
        Self::new(wire.platform, wire.scope).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Session {
    id: SessionId,
    scope: SessionScope,
    created_at_unix: u64,
    updated_at_unix: u64,
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

    pub(crate) fn refresh_from(&mut self, replacement: Self) {
        debug_assert_eq!(self.id, replacement.id);
        debug_assert_eq!(self.scope, replacement.scope);

        self.updated_at_unix = self.updated_at_unix.max(replacement.updated_at_unix);
    }

    pub fn id(&self) -> &SessionId {
        &self.id
    }

    pub fn scope(&self) -> &SessionScope {
        &self.scope
    }

    pub fn created_at_unix(&self) -> u64 {
        self.created_at_unix
    }

    pub fn updated_at_unix(&self) -> u64 {
        self.updated_at_unix
    }

    pub fn validate(&self) -> Result<(), String> {
        self.scope.validate()?;

        let expected_id = SessionId::for_scope(&self.scope);
        if self.id != expected_id {
            return Err(format!(
                "session {} does not match derived id {} for scope",
                self.id, expected_id
            ));
        }

        if self.updated_at_unix < self.created_at_unix {
            return Err(format!(
                "session {} has updated_at_unix before created_at_unix",
                self.id
            ));
        }

        Ok(())
    }
}

impl<'de> Deserialize<'de> for Session {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SessionWire {
            id: SessionId,
            scope: SessionScope,
            created_at_unix: u64,
            updated_at_unix: u64,
        }

        let wire = SessionWire::deserialize(deserializer)?;
        let session = Self {
            id: wire.id,
            scope: wire.scope,
            created_at_unix: wire.created_at_unix,
            updated_at_unix: wire.updated_at_unix,
        };

        session.validate().map_err(de::Error::custom)?;
        Ok(session)
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

fn encode_canonical_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = format!("{:x}_", value.len());

    for byte in value.as_bytes() {
        encoded.push(char::from(HEX[(byte >> 4) as usize]));
        encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
    }

    encoded
}

#[cfg(test)]
mod tests {
    use super::{Session, SessionId, SessionScope};

    #[test]
    fn session_id_is_stable_for_scope() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let first = Session::new(scope.clone());
        let second = Session::new(scope);

        assert_eq!(first.id(), second.id());
        assert_eq!(
            first.id().as_str(),
            "session_v1_4_6c61726b_b_636861743a6f635f313233"
        );
    }

    #[test]
    fn session_id_preserves_scope_boundaries_without_hash_collisions() {
        let first = SessionScope::new("a", "bc").expect("valid scope");
        let second = SessionScope::new("ab", "c").expect("valid scope");

        assert_ne!(SessionId::for_scope(&first), SessionId::for_scope(&second));
    }

    #[test]
    fn rejects_invalid_session_ids() {
        assert!(SessionId::new("").is_err());
        assert!(SessionId::new("bad id").is_err());
        assert!(SessionId::new("session_ok-1").is_ok());
    }

    #[test]
    fn rejects_invalid_session_ids_from_json() {
        let err = serde_json::from_str::<SessionId>("\"bad id\"")
            .expect_err("invalid session id json should fail");

        assert!(err.to_string().contains("invalid session id"));
    }

    #[test]
    fn rejects_invalid_session_scopes_from_json() {
        let err = serde_json::from_str::<SessionScope>(r#"{"platform":"","scope":"chat:oc_123"}"#)
            .expect_err("empty platform json should fail");

        assert!(
            err.to_string()
                .contains("session platform must not be empty")
        );

        let err = serde_json::from_str::<SessionScope>(r#"{"platform":"lark","scope":"  "}"#)
            .expect_err("empty scope json should fail");

        assert!(err.to_string().contains("session scope must not be empty"));
    }

    #[test]
    fn rejects_session_id_scope_mismatch_from_json() {
        let err = serde_json::from_str::<Session>(
            r#"{
                "id": "session_wrong",
                "scope": {"platform": "lark", "scope": "chat:oc_123"},
                "created_at_unix": 1,
                "updated_at_unix": 1
            }"#,
        )
        .expect_err("session id mismatch json should fail");

        assert!(err.to_string().contains("does not match derived id"));
    }

    #[test]
    fn rejects_session_time_order_from_json() {
        let err = serde_json::from_str::<Session>(
            r#"{
                "id": "session_v1_4_6c61726b_b_636861743a6f635f313233",
                "scope": {"platform": "lark", "scope": "chat:oc_123"},
                "created_at_unix": 100,
                "updated_at_unix": 1
            }"#,
        )
        .expect_err("session updated_at before created_at should fail");

        assert!(
            err.to_string()
                .contains("updated_at_unix before created_at_unix")
        );
    }
}
