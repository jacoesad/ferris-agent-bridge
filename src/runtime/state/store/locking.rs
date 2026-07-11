use std::{
    fs,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

static STATE_STORE_WRITE_LOCK: OnceLock<Arc<Mutex<()>>> = OnceLock::new();

pub(super) fn write_lock_for_path(_path: &Path) -> Arc<Mutex<()>> {
    STATE_STORE_WRITE_LOCK
        .get_or_init(|| Arc::new(Mutex::new(())))
        .clone()
}

pub(super) fn state_store_lock_key(path: &Path) -> PathBuf {
    let mut normalized = absolute_path(path);

    loop {
        let next = normalize_from_deepest_existing_ancestor(&normalized);
        if next == normalized {
            return next;
        }
        normalized = next;
    }
}

fn absolute_path(path: &Path) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(path)
        }
    })
}

fn normalize_from_deepest_existing_ancestor(path: &Path) -> PathBuf {
    let mut ancestor = path.to_path_buf();

    loop {
        if let Ok(canonical_ancestor) = fs::canonicalize(&ancestor) {
            let missing_tail = path.strip_prefix(&ancestor).unwrap_or(Path::new(""));
            return normalize_lexically(&canonical_ancestor.join(missing_tail));
        }

        if !ancestor.pop() {
            return normalize_lexically(path);
        }
    }
}

fn normalize_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if normalized.file_name().is_some() {
                    normalized.pop();
                } else if !normalized.has_root() {
                    normalized.push(component.as_os_str());
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }

    normalized
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::super::StateStore;
    use crate::runtime::state::RuntimeState;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn state_store_serializes_writes_process_wide() {
        let root = test_path("state-process-write-lock");
        let first = StateStore::new(root.join("first.state.json"));
        let second = StateStore::new(root.join("second.state.json"));

        assert!(first.shares_write_lock_with(&second));
    }
    #[test]
    fn state_store_normalizes_missing_parent_traversal_for_io_and_locking() {
        let root = test_path("state-missing-parent-alias");
        let missing_parent = root.join("new");
        let direct_path = missing_parent.join("runtime.state.json");
        let alias_path = missing_parent
            .join("..")
            .join("new")
            .join("runtime.state.json");
        assert!(!missing_parent.exists());

        let alias_store = StateStore::new(alias_path);
        let direct_store = StateStore::new(direct_path);

        assert_eq!(alias_store.path(), direct_store.path());
        assert!(alias_store.shares_write_lock_with(&direct_store));
        let expected = RuntimeState::new();
        direct_store
            .save(&expected)
            .expect("direct path should save");
        assert_eq!(
            alias_store.load().expect("alias path should load"),
            expected
        );
    }
    #[test]
    #[cfg(windows)]
    fn state_store_serializes_case_variant_missing_windows_paths() {
        let root = test_path("state-missing-parent-case-alias");
        assert!(!root.join("New").exists());
        let upper_store = StateStore::new(root.join("New").join("runtime.state.json"));
        let lower_store = StateStore::new(root.join("new").join("runtime.state.json"));

        assert!(upper_store.shares_write_lock_with(&lower_store));
    }
    #[test]
    #[cfg(unix)]
    fn state_store_serializes_dangling_symlink_aliases_after_target_creation() {
        let root = test_path("state-dangling-symlink-alias");
        let target_parent = root.join("target");
        let linked_parent = root.join("linked");
        std::os::unix::fs::symlink(&target_parent, &linked_parent)
            .expect("dangling parent symlink should be created");

        let linked_store = StateStore::new(linked_parent.join("runtime.state.json"));
        let direct_store = StateStore::new(target_parent.join("runtime.state.json"));

        assert!(linked_store.shares_write_lock_with(&direct_store));
        let expected = RuntimeState::new();
        direct_store
            .save(&expected)
            .expect("direct path should create the symlink target");
        assert_eq!(
            linked_store
                .load()
                .expect("linked path should resolve after target creation"),
            expected
        );
    }
    #[test]
    #[cfg(unix)]
    fn state_store_normalizes_symlinked_parent_paths_for_io_and_locking() {
        let root = test_path("state-symlink-path-lock");
        let real_parent = root.join("real");
        let linked_parent = root.join("linked");
        fs::create_dir(&real_parent).expect("real parent should exist");
        std::os::unix::fs::symlink(&real_parent, &linked_parent)
            .expect("parent symlink should be created");

        let real_store = StateStore::new(real_parent.join("runtime.state.json"));
        let linked_store = StateStore::new(linked_parent.join("runtime.state.json"));

        assert_eq!(real_store.path(), linked_store.path());
        assert!(real_store.shares_write_lock_with(&linked_store));
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
