use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process,
};

use super::{
    DaemonPaths, DaemonRecord,
    process_ops::{now_unix_nanos, now_unix_seconds},
};

pub(super) fn open_log_file(paths: &DaemonPaths) -> Result<fs::File, String> {
    open_private_append_file(&paths.log_file).map_err(|err| {
        format!(
            "failed to open daemon log {}: {err}",
            paths.log_file.display()
        )
    })
}

pub(super) fn append_log(paths: &DaemonPaths, message: &str) -> Result<(), String> {
    let line = format!("{} {message}\n", now_unix_seconds());
    open_private_append_file(&paths.log_file)
        .and_then(|mut file| file.write_all(line.as_bytes()))
        .map_err(|err| format!("failed to write daemon log: {err}"))
}

pub(super) fn read_record(path: &Path) -> Result<Option<DaemonRecord>, String> {
    match fs::read_to_string(path) {
        Ok(content) => DaemonRecord::decode(&content).map(Some),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(format!("failed to read {}: {err}", path.display())),
    }
}

pub(super) fn write_record(path: &Path, record: &DaemonRecord) -> Result<(), String> {
    let tmp = tmp_record_path(path);
    write_private_file(&tmp, &record.encode())
        .map_err(|err| format!("failed to write {}: {err}", tmp.display()))?;
    replace_file(&tmp, path).map_err(|err| {
        let _ = fs::remove_file(&tmp);
        format!("failed to replace {}: {err}", path.display())
    })
}

pub(super) fn write_new_record(path: &Path, record: &DaemonRecord) -> io::Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    configure_private_file_options(&mut options);
    let mut file = options.open(path)?;

    file.write_all(record.encode().as_bytes())?;
    set_private_file_permissions(path)
}

fn tmp_record_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("record");

    path.with_file_name(format!(
        ".{file_name}.tmp-{}-{}",
        process::id(),
        now_unix_nanos()
    ))
}

fn replace_file(src: &Path, dst: &Path) -> io::Result<()> {
    match fs::rename(src, dst) {
        Ok(()) => set_private_file_permissions(dst),
        Err(err) if cfg!(windows) && err.kind() == io::ErrorKind::AlreadyExists => {
            fs::remove_file(dst)?;
            fs::rename(src, dst)?;
            set_private_file_permissions(dst)
        }
        Err(err) => Err(err),
    }
}

pub(super) fn write_private_file(path: &Path, contents: &str) -> io::Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    configure_private_file_options(&mut options);
    let mut file = options.open(path)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    set_private_file_permissions(path)
}

pub(super) fn open_private_append_file(path: &Path) -> io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.create(true).append(true);
    configure_private_file_options(&mut options);
    let file = options.open(path)?;
    set_private_file_permissions(path)?;
    Ok(file)
}

#[cfg(unix)]
pub(super) fn create_private_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    builder.mode(0o700);
    builder.create(path)
}

#[cfg(not(unix))]
pub(super) fn create_private_dir(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)
}

#[cfg(unix)]
pub(super) fn set_private_dir_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
pub(super) fn set_private_dir_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
pub(super) fn configure_private_file_options(options: &mut fs::OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;

    options.mode(0o600);
}

#[cfg(not(unix))]
pub(super) fn configure_private_file_options(_options: &mut fs::OpenOptions) {}

#[cfg(unix)]
pub(super) fn set_private_file_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
pub(super) fn set_private_file_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}

pub(super) fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}
