use std::{
    fs,
    fs::{File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Serialize, de::DeserializeOwned};

static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

pub(super) fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, String> {
    let input = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;

    serde_json::from_str(&input).map_err(|err| format!("failed to parse {}: {err}", path.display()))
}

pub(super) fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    if let Some(parent) = non_empty_parent(path) {
        ensure_private_parent_dir(parent)?;
    }

    let temp_path = temp_path_for(path);
    let mut encoded = serde_json::to_vec_pretty(value)?;
    encoded.push(b'\n');

    let write_result = (|| {
        let mut file = open_private_new_file(&temp_path)?;
        set_private_file_permissions(&file)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        drop(file);
        replace_file(&temp_path, path)?;
        sync_parent(path)?;
        Ok(())
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    write_result
}

#[cfg(not(windows))]
fn replace_file(src: &Path, dst: &Path) -> io::Result<()> {
    fs::rename(src, dst)
}

#[cfg(windows)]
fn replace_file(src: &Path, dst: &Path) -> io::Result<()> {
    use std::{ffi::OsStr, os::windows::ffi::OsStrExt};

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

    unsafe extern "system" {
        fn MoveFileExW(src: *const u16, dst: *const u16, flags: u32) -> i32;
    }

    fn wide_null(value: &OsStr) -> Vec<u16> {
        value.encode_wide().chain(std::iter::once(0)).collect()
    }

    let src = wide_null(src.as_os_str());
    let dst = wide_null(dst.as_os_str());
    let flags = MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH;

    // SAFETY: both paths are null-terminated UTF-16 buffers that live for the
    // duration of the call, and MoveFileExW does not retain the pointers.
    match unsafe { MoveFileExW(src.as_ptr(), dst.as_ptr(), flags) } {
        0 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

fn temp_path_for(path: &Path) -> PathBuf {
    let sequence = NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("runtime-state");

    path.with_file_name(format!(
        ".{file_name}.tmp.{}.{}.{}",
        process::id(),
        nanos,
        sequence
    ))
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = non_empty_parent(path) {
        File::open(parent)?.sync_all()?;
    }

    Ok(())
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn non_empty_parent(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
}

fn ensure_private_parent_dir(path: &Path) -> io::Result<()> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} exists and is not a directory", path.display()),
        )),
        Err(err) if err.kind() == io::ErrorKind::NotFound => create_private_dir(path),
        Err(err) => Err(err),
    }
}

#[cfg(unix)]
fn create_private_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    builder.mode(0o700);
    builder.create(path)
}

#[cfg(not(unix))]
fn create_private_dir(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)
}

fn open_private_new_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    configure_private_file_options(&mut options);
    options.open(path)
}

#[cfg(unix)]
fn configure_private_file_options(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_file_options(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn set_private_file_permissions(file: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = file.metadata()?.permissions();
    permissions.set_mode(0o600);
    file.set_permissions(permissions)
}

#[cfg(not(unix))]
fn set_private_file_permissions(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::open_private_new_file;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    #[test]
    #[cfg(unix)]
    fn private_new_file_uses_private_mode_at_create_time() {
        use std::os::unix::fs::PermissionsExt;

        let path = test_path("private-new-file").join("secret.tmp");
        let file = open_private_new_file(&path).expect("private file should be created");

        let mode = file
            .metadata()
            .expect("file metadata should load")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn relative_file_path_has_no_parent_directory_to_create() {
        assert!(super::non_empty_parent(std::path::Path::new("runtime.state.json")).is_none());
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
