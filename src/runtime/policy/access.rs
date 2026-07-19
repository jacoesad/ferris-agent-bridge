use std::collections::{BTreeMap, BTreeSet};

use crate::runtime::session::{SessionId, SessionScope};

use super::PolicyDecision;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AccessPrincipal {
    platform: String,
    subject: String,
}

impl AccessPrincipal {
    pub fn new(platform: impl Into<String>, subject: impl Into<String>) -> Result<Self, String> {
        let principal = Self {
            platform: platform.into(),
            subject: subject.into(),
        };

        if principal.platform.trim().is_empty() {
            return Err("access principal platform must not be empty".to_owned());
        }

        if principal.subject.trim().is_empty() {
            return Err("access principal subject must not be empty".to_owned());
        }

        Ok(principal)
    }

    pub fn platform(&self) -> &str {
        &self.platform
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessAction {
    InvokeAgent,
    AdministerBridge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessDenialReason {
    PrincipalNotAllowed,
    ScopeNotAllowed,
    AdministratorRequired,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccessPolicy {
    allowed_principals: BTreeSet<AccessPrincipal>,
    allowed_scopes: BTreeMap<SessionId, SessionScope>,
    administrators: BTreeSet<AccessPrincipal>,
}

impl AccessPolicy {
    pub fn new(
        allowed_principals: impl IntoIterator<Item = AccessPrincipal>,
        allowed_scopes: impl IntoIterator<Item = SessionScope>,
        administrators: impl IntoIterator<Item = AccessPrincipal>,
    ) -> Result<Self, String> {
        let allowed_principals = allowed_principals.into_iter().collect::<BTreeSet<_>>();
        let administrators = administrators.into_iter().collect::<BTreeSet<_>>();

        if !administrators.is_subset(&allowed_principals) {
            return Err("every administrator must also be an allowed principal".to_owned());
        }

        let allowed_scopes = allowed_scopes
            .into_iter()
            .map(|scope| (SessionId::for_scope(&scope), scope))
            .collect();

        Ok(Self {
            allowed_principals,
            allowed_scopes,
            administrators,
        })
    }

    pub fn evaluate(
        &self,
        principal: &AccessPrincipal,
        scope: &SessionScope,
        action: AccessAction,
    ) -> PolicyDecision<AccessDenialReason> {
        if !self.allowed_principals.contains(principal) {
            return PolicyDecision::Denied(AccessDenialReason::PrincipalNotAllowed);
        }

        if !self
            .allowed_scopes
            .contains_key(&SessionId::for_scope(scope))
        {
            return PolicyDecision::Denied(AccessDenialReason::ScopeNotAllowed);
        }

        if action == AccessAction::AdministerBridge && !self.administrators.contains(principal) {
            return PolicyDecision::Denied(AccessDenialReason::AdministratorRequired);
        }

        PolicyDecision::Allowed
    }

    pub fn allowed_principals(&self) -> impl ExactSizeIterator<Item = &AccessPrincipal> {
        self.allowed_principals.iter()
    }

    pub fn allowed_scopes(&self) -> impl ExactSizeIterator<Item = &SessionScope> {
        self.allowed_scopes.values()
    }

    pub fn administrators(&self) -> impl ExactSizeIterator<Item = &AccessPrincipal> {
        self.administrators.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::{AccessAction, AccessDenialReason, AccessPolicy, AccessPrincipal};
    use crate::runtime::{PolicyDecision, SessionScope};

    #[test]
    fn access_principals_require_namespaced_non_empty_values() {
        assert!(AccessPrincipal::new("", "user:1").is_err());
        assert!(AccessPrincipal::new("  ", "user:1").is_err());
        assert!(AccessPrincipal::new("lark", "").is_err());
        assert!(AccessPrincipal::new("lark", "  ").is_err());

        let principal = AccessPrincipal::new("lark", "user:1").expect("valid principal");
        assert_eq!(principal.platform(), "lark");
        assert_eq!(principal.subject(), "user:1");
        assert_ne!(
            principal,
            AccessPrincipal::new("slack", "user:1").expect("valid principal")
        );
        assert_ne!(
            AccessPrincipal::new("a", "bc").expect("valid principal"),
            AccessPrincipal::new("ab", "c").expect("valid principal")
        );
    }

    #[test]
    fn empty_access_policy_denies_by_default() {
        let decision = AccessPolicy::default().evaluate(
            &principal("lark", "user:1"),
            &scope("lark", "chat:1"),
            AccessAction::InvokeAgent,
        );

        assert_eq!(
            decision,
            PolicyDecision::Denied(AccessDenialReason::PrincipalNotAllowed)
        );
    }

    #[test]
    fn access_policy_requires_exact_principal_and_scope_membership() {
        let allowed_principal = principal("lark", "user:1");
        let allowed_scope = scope("lark", "chat:1");
        let policy = AccessPolicy::new([allowed_principal.clone()], [allowed_scope.clone()], [])
            .expect("valid access policy");

        assert_eq!(
            policy.evaluate(
                &allowed_principal,
                &allowed_scope,
                AccessAction::InvokeAgent
            ),
            PolicyDecision::Allowed
        );
        assert_eq!(
            policy.evaluate(
                &principal("slack", "user:1"),
                &allowed_scope,
                AccessAction::InvokeAgent,
            ),
            PolicyDecision::Denied(AccessDenialReason::PrincipalNotAllowed)
        );
        assert_eq!(
            policy.evaluate(
                &allowed_principal,
                &scope("lark", "chat:2"),
                AccessAction::InvokeAgent,
            ),
            PolicyDecision::Denied(AccessDenialReason::ScopeNotAllowed)
        );
        assert_eq!(
            policy.evaluate(
                &allowed_principal,
                &scope("slack", "chat:1"),
                AccessAction::InvokeAgent,
            ),
            PolicyDecision::Denied(AccessDenialReason::ScopeNotAllowed)
        );
    }

    #[test]
    fn administrative_access_requires_an_allowed_administrator() {
        let user = principal("lark", "user:1");
        let administrator = principal("lark", "admin:1");
        let allowed_scope = scope("lark", "chat:1");
        let policy = AccessPolicy::new(
            [user.clone(), administrator.clone()],
            [allowed_scope.clone()],
            [administrator.clone()],
        )
        .expect("valid access policy");

        assert_eq!(
            policy.evaluate(&user, &allowed_scope, AccessAction::InvokeAgent),
            PolicyDecision::Allowed
        );
        assert_eq!(
            policy.evaluate(&user, &allowed_scope, AccessAction::AdministerBridge),
            PolicyDecision::Denied(AccessDenialReason::AdministratorRequired)
        );
        assert_eq!(
            policy.evaluate(
                &administrator,
                &allowed_scope,
                AccessAction::AdministerBridge,
            ),
            PolicyDecision::Allowed
        );
        assert_eq!(
            policy.evaluate(
                &user,
                &scope("lark", "chat:2"),
                AccessAction::AdministerBridge,
            ),
            PolicyDecision::Denied(AccessDenialReason::ScopeNotAllowed)
        );
    }

    #[test]
    fn access_policy_rejects_administrators_outside_the_allowed_set() {
        let result = AccessPolicy::new(
            [principal("lark", "user:1")],
            [scope("lark", "chat:1")],
            [principal("lark", "admin:1")],
        );

        assert_eq!(
            result.expect_err("administrator subset must be enforced"),
            "every administrator must also be an allowed principal"
        );
    }

    #[test]
    fn access_policy_deduplicates_membership_sets() {
        let administrator = principal("lark", "admin:1");
        let allowed_scope = scope("lark", "chat:1");
        let policy = AccessPolicy::new(
            [administrator.clone(), administrator.clone()],
            [allowed_scope.clone(), allowed_scope],
            [administrator.clone(), administrator],
        )
        .expect("valid access policy");

        assert_eq!(policy.allowed_principals().len(), 1);
        assert_eq!(policy.allowed_scopes().len(), 1);
        assert_eq!(policy.administrators().len(), 1);
    }

    fn principal(platform: &str, subject: &str) -> AccessPrincipal {
        AccessPrincipal::new(platform, subject).expect("valid principal")
    }

    fn scope(platform: &str, value: &str) -> SessionScope {
        SessionScope::new(platform, value).expect("valid scope")
    }
}
