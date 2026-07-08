use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

static STATE_STORE_WRITE_LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<Mutex<()>>>>> =
    OnceLock::new();

pub(super) fn write_lock_for_path(path: &Path) -> Arc<Mutex<()>> {
    let key = state_store_lock_key(path);
    let registry = STATE_STORE_WRITE_LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut locks = match registry.lock() {
        Ok(locks) => locks,
        Err(poisoned) => poisoned.into_inner(),
    };

    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return lock;
    }

    let lock = Arc::new(Mutex::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}

pub(super) fn state_store_lock_key(path: &Path) -> PathBuf {
    if let Ok(canonical_path) = fs::canonicalize(path) {
        return canonical_path;
    }

    if let Some(parent) = non_empty_parent(path) {
        if let (Ok(canonical_parent), Some(file_name)) =
            (fs::canonicalize(parent), path.file_name())
        {
            return canonical_parent.join(file_name);
        }
    }

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

fn non_empty_parent(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::super::StateStore;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn state_store_uses_same_process_write_lock_for_same_path() {
        let path = test_path("state-same-path-lock").join("runtime.state.json");
        let first = StateStore::new(&path);
        let second = StateStore::new(&path);

        assert!(first.shares_write_lock_with(&second));
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
