use std::{
    collections::BTreeSet,
    ffi::OsString,
    path::{Component, Path, PathBuf, Prefix},
};

use super::PolicyDecision;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceRoot(OsString);

impl WorkspaceRoot {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        if !path.is_absolute() {
            return Err("workspace root must be absolute".to_owned());
        }

        let mut has_normal_component = false;
        for component in path.components() {
            match component {
                Component::ParentDir => {
                    return Err(
                        "workspace root must not contain parent-directory components".to_owned(),
                    );
                }
                Component::Normal(_) => has_normal_component = true,
                Component::Prefix(prefix)
                    if !matches!(
                        prefix.kind(),
                        Prefix::Disk(_)
                            | Prefix::UNC(_, _)
                            | Prefix::VerbatimDisk(_)
                            | Prefix::VerbatimUNC(_, _)
                    ) =>
                {
                    return Err(
                        "workspace root must use a disk or UNC filesystem path namespace"
                            .to_owned(),
                    );
                }
                _ => {}
            }
        }

        if !has_normal_component {
            return Err("workspace root must not be a filesystem root".to_owned());
        }

        Ok(Self(path.into_os_string()))
    }

    pub fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceDenialReason {
    InvalidRoot,
    RootNotAllowed,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkspacePolicy {
    allowed_roots: BTreeSet<WorkspaceRoot>,
}

impl WorkspacePolicy {
    pub fn new(allowed_roots: impl IntoIterator<Item = WorkspaceRoot>) -> Self {
        Self {
            allowed_roots: allowed_roots.into_iter().collect(),
        }
    }

    pub fn evaluate(&self, requested_root: &Path) -> PolicyDecision<WorkspaceDenialReason> {
        let Ok(requested_root) = WorkspaceRoot::new(requested_root.to_path_buf()) else {
            return PolicyDecision::Denied(WorkspaceDenialReason::InvalidRoot);
        };

        if self.allowed_roots.contains(&requested_root) {
            PolicyDecision::Allowed
        } else {
            PolicyDecision::Denied(WorkspaceDenialReason::RootNotAllowed)
        }
    }

    pub fn allowed_roots(&self) -> impl ExactSizeIterator<Item = &WorkspaceRoot> {
        self.allowed_roots.iter()
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{WorkspaceDenialReason, WorkspacePolicy, WorkspaceRoot};
    use crate::runtime::policy::PolicyDecision;

    #[test]
    fn workspace_roots_require_safe_absolute_non_root_paths() {
        assert!(WorkspaceRoot::new("relative/workspace").is_err());
        assert!(WorkspaceRoot::new(absolute_path("allowed").join("..").join("other")).is_err());
        assert!(WorkspaceRoot::new(filesystem_root()).is_err());
    }

    #[test]
    fn workspace_roots_preserve_lexical_identity() {
        let path = absolute_path(".").join("workspace");
        let root = WorkspaceRoot::new(&path).expect("absolute workspace root should be valid");
        let policy = WorkspacePolicy::new([root.clone()]);

        assert_eq!(root.as_path().as_os_str(), path.as_os_str());
        assert_eq!(policy.evaluate(&path), PolicyDecision::Allowed);
        assert_eq!(
            policy.evaluate(&absolute_path("workspace")),
            PolicyDecision::Denied(WorkspaceDenialReason::RootNotAllowed)
        );
    }

    #[test]
    fn empty_workspace_policy_denies_by_default() {
        let decision = WorkspacePolicy::default().evaluate(&absolute_path("denied"));

        assert_eq!(
            decision,
            PolicyDecision::Denied(WorkspaceDenialReason::RootNotAllowed)
        );
    }

    #[test]
    fn workspace_policy_distinguishes_invalid_roots() {
        let decision = WorkspacePolicy::default().evaluate(Path::new("relative/workspace"));

        assert_eq!(
            decision,
            PolicyDecision::Denied(WorkspaceDenialReason::InvalidRoot)
        );
    }

    #[test]
    fn workspace_policy_allows_only_exact_registered_roots() {
        let allowed_path = absolute_path("allowed");
        let allowed_root =
            WorkspaceRoot::new(&allowed_path).expect("absolute workspace root should be valid");
        let policy = WorkspacePolicy::new([allowed_root.clone(), allowed_root]);

        assert_eq!(policy.evaluate(&allowed_path), PolicyDecision::Allowed);
        assert_eq!(policy.allowed_roots().len(), 1);
        assert_eq!(
            policy.evaluate(&allowed_path.join("child")),
            PolicyDecision::Denied(WorkspaceDenialReason::RootNotAllowed)
        );
        assert_eq!(
            policy.evaluate(&absolute_path("allowed-copy")),
            PolicyDecision::Denied(WorkspaceDenialReason::RootNotAllowed)
        );
    }

    #[test]
    fn workspace_policy_preserves_roots_with_drive_like_normal_components() {
        let first_path = raw_child_path(&absolute_path("first"), "D:tail");
        let second_path = raw_child_path(&absolute_path("second"), "D:tail");
        assert!(first_path.is_absolute());
        assert!(second_path.is_absolute());
        assert_ne!(first_path, second_path);

        let first_root =
            WorkspaceRoot::new(&first_path).expect("absolute workspace root should remain valid");
        let policy = WorkspacePolicy::new([first_root.clone()]);

        assert_eq!(first_root.as_path(), first_path);
        assert_eq!(policy.evaluate(&first_path), PolicyDecision::Allowed);
        assert_eq!(
            policy.evaluate(&second_path),
            PolicyDecision::Denied(WorkspaceDenialReason::RootNotAllowed)
        );
    }

    #[cfg(windows)]
    #[test]
    fn workspace_policy_preserves_distinct_unc_and_verbatim_unc_roots() {
        let unc_path = PathBuf::from(r"\\server\share\policy\workspace");
        let verbatim_unc_path = PathBuf::from(r"\\?\UNC\server\share\policy\workspace");
        let unc_root =
            WorkspaceRoot::new(&unc_path).expect("UNC workspace root should remain valid");
        let policy = WorkspacePolicy::new([unc_root.clone()]);

        assert_eq!(unc_root.as_path(), unc_path);
        assert_eq!(policy.evaluate(&unc_path), PolicyDecision::Allowed);
        assert_eq!(
            policy.evaluate(&verbatim_unc_path),
            PolicyDecision::Denied(WorkspaceDenialReason::RootNotAllowed)
        );
    }

    #[cfg(windows)]
    #[test]
    fn workspace_policy_preserves_distinct_windows_verbatim_roots() {
        let first_path = PathBuf::from(r"\\?\C:\policy\workspace");
        let second_path = PathBuf::from(r"\\?\D:\policy\workspace");
        let first_root =
            WorkspaceRoot::new(&first_path).expect("verbatim workspace root should remain valid");
        let policy = WorkspacePolicy::new([first_root.clone()]);

        assert_eq!(first_root.as_path(), first_path);
        assert_eq!(policy.evaluate(&first_path), PolicyDecision::Allowed);
        assert_eq!(
            policy.evaluate(&second_path),
            PolicyDecision::Denied(WorkspaceDenialReason::RootNotAllowed)
        );
    }

    #[cfg(windows)]
    #[test]
    fn workspace_policy_preserves_windows_lexical_alias_identity() {
        for (first_path, second_path) in [
            (
                PathBuf::from(r"C:\policy\workspace"),
                PathBuf::from(r"C:/policy/workspace"),
            ),
            (
                PathBuf::from(r"\\server\share\policy\workspace"),
                PathBuf::from(r"\\server\share\policy/workspace"),
            ),
            (
                PathBuf::from(r"C:\policy\workspace"),
                PathBuf::from(r"C:\policy\workspace\."),
            ),
            (
                PathBuf::from(r"C:\policy\workspace"),
                PathBuf::from(r"C:\policy\workspace\"),
            ),
            (
                PathBuf::from(r"\\server\share\policy\workspace"),
                PathBuf::from(r"\\server\share\policy\workspace\."),
            ),
            (
                PathBuf::from(r"\\server\share\policy\workspace"),
                PathBuf::from(r"\\server\share\policy\workspace\"),
            ),
        ] {
            assert!(first_path.components().eq(second_path.components()));
            assert_cross_denied_exact_roots(&first_path, &second_path);
        }
    }

    #[cfg(windows)]
    #[test]
    fn workspace_roots_handle_verbatim_current_directory_components() {
        for (path, path_without_current_directory) in [
            (
                PathBuf::from(r"\\?\C:\policy\workspace\."),
                PathBuf::from(r"\\?\C:\policy\workspace"),
            ),
            (
                PathBuf::from(r"\\?\UNC\server\share\policy\workspace\."),
                PathBuf::from(r"\\?\UNC\server\share\policy\workspace"),
            ),
        ] {
            assert_cross_denied_exact_roots(&path, &path_without_current_directory);
        }

        for path in [
            PathBuf::from(r"\\?\C:\."),
            PathBuf::from(r"\\?\UNC\server\share\."),
        ] {
            assert!(WorkspaceRoot::new(path).is_err());
        }
    }

    #[cfg(windows)]
    #[test]
    fn workspace_roots_reject_non_filesystem_windows_namespaces() {
        for path in [
            PathBuf::from(r"\\.\COM42\child"),
            PathBuf::from(r"\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy1\child"),
        ] {
            assert!(path.is_absolute());
            assert!(
                path.components()
                    .any(|component| matches!(component, std::path::Component::Normal(_)))
            );
            assert_eq!(
                WorkspaceRoot::new(path).expect_err("non-filesystem namespace must be rejected"),
                "workspace root must use a disk or UNC filesystem path namespace"
            );
        }
    }

    fn absolute_path(name: &str) -> PathBuf {
        std::env::temp_dir()
            .join("ferris-agent-bridge-policy-tests")
            .join(name)
    }

    fn raw_child_path(base: &Path, component: &str) -> PathBuf {
        let mut raw = base.as_os_str().to_os_string();
        raw.push(std::path::MAIN_SEPARATOR_STR);
        raw.push(component);
        PathBuf::from(raw)
    }

    #[cfg(windows)]
    fn assert_cross_denied_exact_roots(first_path: &Path, second_path: &Path) {
        assert!(first_path.is_absolute());
        assert!(second_path.is_absolute());
        assert_ne!(first_path.as_os_str(), second_path.as_os_str());

        let first_root =
            WorkspaceRoot::new(first_path).expect("absolute workspace root should remain valid");
        let second_root =
            WorkspaceRoot::new(second_path).expect("absolute workspace root should remain valid");
        let first_policy = WorkspacePolicy::new([first_root.clone()]);
        let second_policy = WorkspacePolicy::new([second_root.clone()]);

        assert_eq!(first_root.as_path().as_os_str(), first_path.as_os_str());
        assert_eq!(second_root.as_path().as_os_str(), second_path.as_os_str());
        assert_eq!(first_policy.evaluate(first_path), PolicyDecision::Allowed);
        assert_eq!(
            first_policy.evaluate(second_path),
            PolicyDecision::Denied(WorkspaceDenialReason::RootNotAllowed)
        );
        assert_eq!(second_policy.evaluate(second_path), PolicyDecision::Allowed);
        assert_eq!(
            second_policy.evaluate(first_path),
            PolicyDecision::Denied(WorkspaceDenialReason::RootNotAllowed)
        );
    }

    fn filesystem_root() -> PathBuf {
        std::env::temp_dir()
            .ancestors()
            .last()
            .expect("an absolute temp directory should have a filesystem root")
            .to_path_buf()
    }
}
