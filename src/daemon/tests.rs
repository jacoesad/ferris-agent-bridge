use super::*;

#[test]
fn record_roundtrips() {
    let record = DaemonRecord::new(
        42,
        "token".to_owned(),
        "/tmp/fab".to_owned(),
        MODE_BACKGROUND,
    );

    let decoded = DaemonRecord::decode(&record.encode()).expect("record should decode");

    assert_eq!(decoded, record);
}

#[test]
fn record_decode_rejects_empty_identity_fields() {
    for input in [
        "pid=1\ntoken=\nexe=/tmp/fab\nstarted_at_unix=1\nmode=background\n",
        "pid=1\ntoken=token\nexe=\nstarted_at_unix=1\nmode=background\n",
        "pid=1\ntoken=token\nexe=/tmp/fab\nstarted_at_unix=1\nmode=\n",
        "pid=1\ntoken=token\nexe=/tmp/fab\nstarted_at_unix=1\nmode=other\n",
    ] {
        assert!(
            DaemonRecord::decode(input).is_err(),
            "record should reject invalid input: {input:?}"
        );
    }
}

#[test]
fn lifecycle_timeout_budget_allows_stale_recovery() {
    assert!(
        LIFECYCLE_LOCK_STALE_TTL > STOP_TIMEOUT + STOP_TIMEOUT,
        "lifecycle lock stale TTL should exceed graceful stop plus force-stop wait"
    );
    assert!(
        LIFECYCLE_LOCK_TIMEOUT > LIFECYCLE_LOCK_STALE_TTL,
        "lifecycle lock wait timeout should let a waiter reach stale recovery"
    );
    assert!(
        STARTING_LOCK_TTL > START_TIMEOUT,
        "starting lock TTL should outlive normal start readiness wait"
    );
}

#[test]
fn status_is_stopped_without_state() {
    let dir = temp_test_dir("status-stopped");
    let paths = DaemonPaths::new(&dir);

    assert_eq!(inspect_status(&paths), DaemonStatus::Stopped);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn missing_state_falls_back_to_starting_lock() {
    let dir = temp_test_dir("status-starting-lock");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");
    let starting = DaemonRecord::new(
        process::id(),
        "token".to_owned(),
        current_exe_string(),
        MODE_STARTING,
    );
    write_new_record(&paths.lock_file, &starting).expect("starting lock should be written");

    assert!(matches!(inspect_status(&paths), DaemonStatus::Starting(_)));
    let err = stop(&paths).expect_err("stop should not report starting daemon as stopped");
    assert!(err.contains("startup is in progress"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn stale_starting_lock_with_live_pid_is_recovered() {
    let dir = temp_test_dir("stale-starting-live-pid");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");
    let mut starting = DaemonRecord::new(
        process::id(),
        "stale-token".to_owned(),
        current_exe_string(),
        MODE_STARTING,
    );
    starting.started_at_unix = 1;
    write_new_record(&paths.lock_file, &starting).expect("starting lock should be written");

    let preparation = prepare_start(&paths).expect("stale starting lock should not block forever");
    let token = match preparation {
        StartPreparation::Ready {
            token,
            lifecycle_lock,
        } => {
            drop(lifecycle_lock);
            token
        }
        StartPreparation::AlreadyRunning(record) => {
            panic!("stale starting lock should not report running: {record:?}")
        }
    };
    let next = read_record(&paths.lock_file)
        .expect("lock should read")
        .expect("lock should exist");

    assert_eq!(next.token, token);
    assert_eq!(next.mode, MODE_STARTING);
    cleanup_runtime_files_for_token(&paths, &token);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn status_reports_stale_dead_pid() {
    let dir = temp_test_dir("status-stale");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");

    let record = DaemonRecord::new(
        u32::MAX,
        "token".to_owned(),
        "missing".to_owned(),
        MODE_BACKGROUND,
    );
    write_record(&paths.state_file, &record).expect("state should be written");

    let status = inspect_status(&paths);
    assert!(matches!(
        status,
        DaemonStatus::Stale {
            pid: Some(u32::MAX),
            ..
        }
    ));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn lock_fallback_mismatched_live_identity_is_unverified() {
    for issue in [
        StateFileIssue::Missing,
        StateFileIssue::Invalid("bad state".to_owned()),
    ] {
        let dir = temp_test_dir("lock-fallback-mismatch");
        let paths = DaemonPaths::new(&dir);
        fs::create_dir_all(&dir).expect("test dir should be created");
        let record = DaemonRecord::new(
            process::id(),
            "token-not-in-command-line".to_owned(),
            "/not/the/current/exe".to_owned(),
            MODE_BACKGROUND,
        );
        write_record(&paths.lock_file, &record).expect("lock should be written");

        match inspect_lock_without_state(&paths, issue) {
            DaemonStatus::RunningUnverified {
                record: actual,
                reason,
            } => {
                assert_eq!(actual, record);
                assert!(reason.contains("daemon state"));
                assert!(reason.contains("process command line does not match"));
            }
            other => panic!("expected running unverified fallback, got {other:?}"),
        }

        let _ = fs::remove_dir_all(dir);
    }
}

#[test]
fn stop_does_not_create_missing_runtime_dir() {
    let dir = temp_test_dir("stop-missing-runtime");
    let paths = DaemonPaths::new(&dir);

    assert!(!dir.exists());
    let output = stop(&paths).expect("stop should succeed without runtime dir");

    assert!(output.contains("daemon is not running"));
    assert!(!dir.exists());
}

#[test]
fn invalid_lock_is_not_replaced() {
    let dir = temp_test_dir("invalid-lock");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");
    fs::write(&paths.lock_file, "pid=").expect("invalid lock should be written");

    let err = acquire_start_lock(&paths, "next-token".to_owned())
        .expect_err("invalid lock should block replacement");

    assert!(err.contains("unsafe"));
    assert_eq!(
        fs::read_to_string(&paths.lock_file).expect("lock should still exist"),
        "pid="
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn stale_lock_is_replaced() {
    let dir = temp_test_dir("stale-lock");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");
    let stale = DaemonRecord::new(
        u32::MAX,
        "stale-token".to_owned(),
        "missing".to_owned(),
        MODE_BACKGROUND,
    );
    write_new_record(&paths.lock_file, &stale).expect("stale lock should be written");

    acquire_start_lock(&paths, "next-token".to_owned()).expect("stale lock should be replaceable");
    let next = read_record(&paths.lock_file)
        .expect("lock should read")
        .expect("lock should exist");

    assert_eq!(next.token, "next-token");
    assert_eq!(next.mode, MODE_STARTING);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn internal_daemon_requires_matching_startup_lock() {
    let dir = temp_test_dir("internal-start-lock");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");
    let active = DaemonRecord::new(
        u32::MAX,
        "active-token".to_owned(),
        "active-exe".to_owned(),
        MODE_BACKGROUND,
    );
    write_record(&paths.lock_file, &active).expect("active lock should be written");
    write_record(&paths.state_file, &active).expect("active state should be written");

    let err = run_daemon(
        &paths,
        "rogue-token".to_owned(),
        process::id(),
        MODE_BACKGROUND,
    )
    .expect_err("internal daemon should require a startup lock");

    assert!(err.contains("startup lock mode"));
    assert_eq!(
        read_record(&paths.lock_file)
            .expect("lock should read")
            .expect("lock should exist"),
        active
    );
    assert_eq!(
        read_record(&paths.state_file)
            .expect("state should read")
            .expect("state should exist"),
        active
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn startup_lock_requires_matching_starter_pid() {
    let dir = temp_test_dir("startup-lock-starter");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");
    let token = "token".to_owned();
    let starting = DaemonRecord::new(
        process::id(),
        token.clone(),
        "starter".to_owned(),
        MODE_STARTING,
    );
    write_new_record(&paths.lock_file, &starting).expect("starting lock should be written");

    validate_startup_lock(&paths, &token, process::id())
        .expect("matching startup lock should be accepted");
    let err = validate_startup_lock(&paths, &token, process::id().saturating_add(1))
        .expect_err("wrong starter pid should be rejected");

    assert!(err.contains("starter pid"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn start_cleanup_guard_removes_startup_files_before_spawn() {
    let dir = temp_test_dir("start-cleanup-guard");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");
    let token = "token".to_owned();
    let record = DaemonRecord::new(
        process::id(),
        token.clone(),
        current_exe_string(),
        MODE_STARTING,
    );

    write_new_record(&paths.lock_file, &record).expect("lock should be written");
    write_record(&paths.state_file, &record).expect("state should be written");
    write_private_file(&paths.stop_file, &token).expect("stop should be written");

    {
        let _guard = StartCleanupGuard::new(&paths, &token);
    }

    assert!(!paths.lock_file.exists());
    assert!(!paths.state_file.exists());
    assert!(!paths.stop_file.exists());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn lifecycle_lock_drop_does_not_remove_replaced_lock() {
    let dir = temp_test_dir("lifecycle-lock-drop");
    fs::create_dir_all(&dir).expect("test dir should be created");
    let lock_file = dir.join(LIFECYCLE_LOCK_FILE);

    let old_guard = create_lifecycle_lock(&lock_file).expect("old lock should be created");
    remove_file_if_exists(&lock_file).expect("stale lock should be removed");
    let new_guard = create_lifecycle_lock(&lock_file).expect("new lock should be created");
    let new_token = new_guard.token.clone();

    drop(old_guard);

    assert_eq!(
        read_lifecycle_lock_token(&lock_file).expect("lock token should read"),
        Some(new_token)
    );

    drop(new_guard);
    assert!(!lock_file.exists());

    let _ = fs::remove_dir_all(dir);
}

#[test]
#[cfg(unix)]
fn stale_lifecycle_lock_with_live_pid_is_not_recovered() {
    let dir = temp_test_dir("live-stale-lifecycle-lock");
    fs::create_dir_all(&dir).expect("test dir should be created");
    let lock_file = dir.join(LIFECYCLE_LOCK_FILE);
    fs::write(&lock_file, lifecycle_lock_contents("live-token")).expect("lock should be written");
    mark_file_stale(&lock_file);

    assert!(matches!(
        inspect_stale_lifecycle_lock(&lock_file),
        StaleLifecycleLock::NotStale
    ));
    assert_eq!(
        read_lifecycle_lock_token(&lock_file).expect("lock token should read"),
        Some("live-token".to_owned())
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
#[cfg(unix)]
fn acquire_lifecycle_lock_recovers_complete_dead_stale_lock() {
    let dir = temp_test_dir("dead-stale-lifecycle-lock");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");
    let lock_file = dir.join(LIFECYCLE_LOCK_FILE);
    fs::write(
        &lock_file,
        format!("pid={}\ntoken=dead-token\nstarted_at_unix=1\n", u32::MAX),
    )
    .expect("lock should be written");
    mark_file_stale(&lock_file);

    let guard = acquire_lifecycle_lock(&paths).expect("dead stale lock should recover");

    assert_eq!(
        read_lifecycle_lock_token(&lock_file).expect("lock token should read"),
        Some(guard.token.clone())
    );

    drop(guard);
    let _ = fs::remove_dir_all(dir);
}

#[test]
#[cfg(unix)]
fn incomplete_lifecycle_lock_cleanup_removes_matching_tokenless_file() {
    let dir = temp_test_dir("incomplete-lifecycle-lock");
    fs::create_dir_all(&dir).expect("test dir should be created");
    let lock_file = dir.join(LIFECYCLE_LOCK_FILE);
    fs::write(&lock_file, format!("pid={}\n", dead_test_pid()))
        .expect("incomplete lock should be written");
    mark_file_stale(&lock_file);
    let snapshot = FileSnapshot::from_path(&lock_file)
        .expect("snapshot should read")
        .expect("snapshot should exist");

    let removed = cleanup_incomplete_lifecycle_lock_if_matches(&lock_file, &snapshot)
        .expect("cleanup should succeed");

    assert!(removed);
    assert!(!lock_file.exists());

    let _ = fs::remove_dir_all(dir);
}

#[test]
#[cfg(unix)]
fn incomplete_lifecycle_lock_cleanup_does_not_remove_live_owner() {
    let dir = temp_test_dir("incomplete-lifecycle-lock-live");
    fs::create_dir_all(&dir).expect("test dir should be created");
    let lock_file = dir.join(LIFECYCLE_LOCK_FILE);
    fs::write(&lock_file, format!("pid={}\n", process::id()))
        .expect("incomplete lock should be written");
    mark_file_stale(&lock_file);
    let snapshot = FileSnapshot::from_path(&lock_file)
        .expect("snapshot should read")
        .expect("snapshot should exist");

    let removed = cleanup_incomplete_lifecycle_lock_if_matches(&lock_file, &snapshot)
        .expect("cleanup should succeed");

    assert!(!removed);
    assert!(lock_file.exists());

    let _ = fs::remove_dir_all(dir);
}

#[test]
#[cfg(unix)]
fn incomplete_lifecycle_lock_cleanup_does_not_remove_replaced_lock() {
    let dir = temp_test_dir("incomplete-lifecycle-lock-replaced");
    fs::create_dir_all(&dir).expect("test dir should be created");
    let lock_file = dir.join(LIFECYCLE_LOCK_FILE);
    fs::write(&lock_file, format!("pid={}\n", dead_test_pid()))
        .expect("incomplete lock should be written");
    mark_file_stale(&lock_file);
    let old_snapshot = FileSnapshot::from_path(&lock_file)
        .expect("snapshot should read")
        .expect("snapshot should exist");

    remove_file_if_exists(&lock_file).expect("old lock should be removed");
    let new_guard = create_lifecycle_lock(&lock_file).expect("new lock should be created");
    let new_token = new_guard.token.clone();

    let removed = cleanup_incomplete_lifecycle_lock_if_matches(&lock_file, &old_snapshot)
        .expect("cleanup should succeed");

    assert!(!removed);
    assert_eq!(
        read_lifecycle_lock_token(&lock_file).expect("lock token should read"),
        Some(new_token)
    );

    drop(new_guard);
    let _ = fs::remove_dir_all(dir);
}

#[test]
#[cfg(unix)]
fn acquire_lifecycle_lock_recovers_tokenless_stale_lock() {
    let dir = temp_test_dir("tokenless-stale-lifecycle-lock");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");
    let lock_file = dir.join(LIFECYCLE_LOCK_FILE);
    fs::write(&lock_file, format!("pid={}\n", dead_test_pid()))
        .expect("incomplete lock should be written");

    mark_file_stale(&lock_file);

    let guard = acquire_lifecycle_lock(&paths).expect("stale tokenless lock should recover");

    assert_eq!(
        read_lifecycle_lock_token(&lock_file).expect("lock token should read"),
        Some(guard.token.clone())
    );

    drop(guard);
    assert!(!lock_file.exists());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn force_stop_refuses_when_state_changed() {
    let dir = temp_test_dir("force-stop-state-changed");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");
    let old = DaemonRecord::new(
        process::id(),
        "old-token".to_owned(),
        current_exe_string(),
        MODE_BACKGROUND,
    );
    let new = DaemonRecord::new(
        process::id(),
        "new-token".to_owned(),
        current_exe_string(),
        MODE_BACKGROUND,
    );
    write_record(&paths.state_file, &new).expect("new state should be written");

    let err =
        force_stop_verified(&paths, &old).expect_err("force stop should reject changed state");

    assert!(err.contains("state changed"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn cleanup_for_old_record_does_not_remove_new_owner_files() {
    let dir = temp_test_dir("cleanup-owner");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");
    let old = DaemonRecord::new(
        u32::MAX - 1,
        "old-token".to_owned(),
        "old-exe".to_owned(),
        MODE_BACKGROUND,
    );
    let new = DaemonRecord::new(
        u32::MAX,
        "new-token".to_owned(),
        "new-exe".to_owned(),
        MODE_BACKGROUND,
    );

    write_record(&paths.lock_file, &new).expect("new lock should be written");
    write_record(&paths.state_file, &new).expect("new state should be written");
    write_private_file(&paths.stop_file, &new.token).expect("new stop file should be written");

    cleanup_runtime_files_for_record(&paths, &old);

    assert_eq!(
        read_record(&paths.lock_file)
            .expect("lock should read")
            .expect("lock should exist"),
        new
    );
    assert_eq!(
        read_record(&paths.state_file)
            .expect("state should read")
            .expect("state should exist"),
        new
    );
    assert_eq!(
        fs::read_to_string(&paths.stop_file).expect("stop should exist"),
        "new-token"
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn matching_exe_without_daemon_token_is_not_owned() {
    let record = DaemonRecord::new(
        process::id(),
        "missing-token".to_owned(),
        current_exe_string(),
        MODE_BACKGROUND,
    );

    assert!(!record_matches_process(&record));
}

#[test]
#[cfg(unix)]
fn failed_start_cleanup_stops_child_and_removes_runtime_files() {
    let dir = temp_test_dir("failed-start-cleanup");
    let paths = DaemonPaths::new(&dir);
    fs::create_dir_all(&dir).expect("test dir should be created");
    let mut child = Command::new("sleep")
        .arg("10")
        .spawn()
        .expect("sleep should start");

    write_new_record(
        &paths.lock_file,
        &DaemonRecord::new(
            child.id(),
            "token".to_owned(),
            "sleep".to_owned(),
            MODE_BACKGROUND,
        ),
    )
    .expect("lock should be written");
    write_record(
        &paths.state_file,
        &DaemonRecord::new(
            child.id(),
            "token".to_owned(),
            "sleep".to_owned(),
            MODE_BACKGROUND,
        ),
    )
    .expect("state should be written");

    cleanup_failed_start(&paths, &mut child, "token");

    assert!(!is_process_running(child.id()));
    assert!(!paths.lock_file.exists());
    assert!(!paths.state_file.exists());
    assert!(!paths.stop_file.exists());

    let _ = fs::remove_dir_all(dir);
}

#[test]
#[cfg(unix)]
fn kill_zero_permission_denied_still_means_process_exists() {
    assert!(kill_zero_stderr_indicates_live_process(
        b"kill: 123: Operation not permitted\n"
    ));
    assert!(!kill_zero_stderr_indicates_live_process(
        b"kill: 123: No such process\n"
    ));
}

fn temp_test_dir(name: &str) -> PathBuf {
    env::temp_dir().join(format!(
        "{NAME}-{name}-{}-{}",
        process::id(),
        generate_token()
    ))
}

#[cfg(unix)]
fn dead_test_pid() -> u32 {
    let pid = u32::MAX;
    assert!(!is_process_running(pid), "test pid should not be live");
    pid
}

#[cfg(unix)]
fn mark_file_stale(path: &Path) {
    let status = Command::new("touch")
        .args(["-t", "197001010000"])
        .arg(path)
        .status()
        .expect("touch should run");
    assert!(status.success(), "touch should mark file as stale");
}
