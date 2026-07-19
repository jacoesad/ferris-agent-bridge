mod access;
mod workspace;

pub use access::{AccessAction, AccessDenialReason, AccessPolicy, AccessPrincipal};
pub use workspace::{WorkspaceDenialReason, WorkspacePolicy, WorkspaceRoot};

#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "policy decisions must be enforced"]
pub enum PolicyDecision<R> {
    Allowed,
    Denied(R),
}

impl<R> PolicyDecision<R> {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }

    pub fn denial_reason(&self) -> Option<&R> {
        match self {
            Self::Allowed => None,
            Self::Denied(reason) => Some(reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PolicyDecision;

    #[test]
    fn policy_decisions_expose_allow_and_typed_denial() {
        let allowed = PolicyDecision::<&str>::Allowed;
        assert!(allowed.is_allowed());
        assert_eq!(allowed.denial_reason(), None);

        let denied = PolicyDecision::Denied("not allowed");
        assert!(!denied.is_allowed());
        assert_eq!(denied.denial_reason(), Some(&"not allowed"));
    }
}
