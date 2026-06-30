#![cfg(target_os = "macos")]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const HOME_ENV: &str = "FERRIS_AGENT_BRIDGE_HOME";
static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn daemon_lifecycle_start_status_duplicate_start_and_stop() {
    let runtime = RuntimeDir::new();

    let start = runtime.run(["start"]);
    assert_success(&start);
    assert!(stdout(&start).contains("daemon started"));

    let duplicate_start = runtime.run(["start"]);
    assert_success(&duplicate_start);
    assert!(stdout(&duplicate_start).contains("already running"));

    let status = runtime.run(["status"]);
    assert_success(&status);
    assert!(stdout(&status).contains("daemon is running"));

    let stop = runtime.run(["stop"]);
    assert_success(&stop);
    assert!(stdout(&stop).contains("daemon stopped"));

    let stopped_status = runtime.run(["status"]);
    assert_success(&stopped_status);
    assert!(stdout(&stopped_status).contains("daemon is stopped"));
}

#[test]
fn concurrent_starts_do_not_create_duplicate_daemons() {
    let runtime = RuntimeDir::new();
    let handles = (0..6)
        .map(|_| {
            let bin = runtime.bin.clone();
            let path = runtime.path.clone();

            thread::spawn(move || {
                Command::new(bin)
                    .arg("start")
                    .env(HOME_ENV, path)
                    .output()
                    .expect("bridge command should run")
            })
        })
        .collect::<Vec<_>>();

    let outputs = handles
        .into_iter()
        .map(|handle| handle.join().expect("start thread should join"))
        .collect::<Vec<_>>();
    let started_count = outputs
        .iter()
        .filter(|output| stdout(output).contains("daemon started"))
        .count();

    assert!(
        started_count <= 1,
        "only one concurrent start may report daemon started\noutputs: {}",
        describe_outputs(&outputs)
    );

    for output in &outputs {
        let text = format!("{}\n{}", stdout(output), stderr(output));
        assert!(
            output.status.success()
                || text.contains("daemon start is already in progress")
                || text.contains("daemon lock is active"),
            "unexpected concurrent start result\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            stdout(output),
            stderr(output)
        );
    }

    let status = runtime.run(["status"]);
    assert_success(&status);
    assert!(stdout(&status).contains("daemon is running"));

    let stop = runtime.run(["stop"]);
    assert_success(&stop);
    assert!(stdout(&stop).contains("daemon stopped"));
}

#[test]
fn runtime_directory_and_files_are_private() {
    let runtime = RuntimeDir::new();

    let start = runtime.run(["start"]);
    assert_success(&start);

    assert_mode(&runtime.path, 0o700);
    assert_mode(&runtime.path.join("daemon.lock"), 0o600);
    assert_mode(&runtime.path.join("daemon.state"), 0o600);
    assert_mode(&runtime.path.join("daemon.log"), 0o600);

    let stop = runtime.run(["stop"]);
    assert_success(&stop);
}

#[test]
fn foreground_daemon_can_be_stopped_from_another_command() {
    let runtime = RuntimeDir::new();
    let mut child = Command::new(&runtime.bin)
        .arg("run")
        .env(HOME_ENV, &runtime.path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("foreground bridge command should start");

    let running = wait_until(Duration::from_secs(2), || {
        let status = runtime.run(["status"]);

        status.status.success() && stdout(&status).contains("daemon is running")
    });
    assert!(running, "foreground daemon should become running");

    let stop = runtime.run(["stop"]);
    assert_success(&stop);
    assert!(stdout(&stop).contains("daemon stopped"));
    assert!(
        wait_for_child_exit(&mut child, Duration::from_secs(2)),
        "foreground command should exit after stop"
    );
}

struct RuntimeDir {
    bin: PathBuf,
    path: PathBuf,
}

impl RuntimeDir {
    fn new() -> Self {
        let bin = PathBuf::from(env!("CARGO_BIN_EXE_ferris-agent-bridge"));
        let path = std::env::temp_dir().join(format!(
            "ferris-agent-bridge-it-{}-{}-{}",
            std::process::id(),
            unique_suffix(),
            NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed)
        ));

        fs::create_dir_all(&path).expect("runtime dir should be created");

        Self { bin, path }
    }

    fn run<const N: usize>(&self, args: [&str; N]) -> Output {
        Command::new(&self.bin)
            .args(args)
            .env(HOME_ENV, &self.path)
            .output()
            .expect("bridge command should run")
    }
}

impl Drop for RuntimeDir {
    fn drop(&mut self) {
        let _ = Command::new(&self.bin)
            .arg("stop")
            .env(HOME_ENV, &self.path)
            .output();
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        stdout(output),
        stderr(output)
    );
}

fn stdout(output: &Output) -> String {
    output_text(&output.stdout)
}

fn stderr(output: &Output) -> String {
    output_text(&output.stderr)
}

fn output_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn describe_outputs(outputs: &[Output]) -> String {
    outputs
        .iter()
        .map(|output| {
            format!(
                "status: {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                stdout(output),
                stderr(output)
            )
        })
        .collect::<Vec<_>>()
        .join("\n---\n")
}

fn assert_mode(path: &std::path::Path, expected: u32) {
    let mode = fs::metadata(path)
        .unwrap_or_else(|err| panic!("{} should exist: {err}", path.display()))
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(mode, expected, "unexpected mode for {}", path.display());
}

fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let started = Instant::now();

    while started.elapsed() < timeout {
        if condition() {
            return true;
        }

        thread::sleep(Duration::from_millis(100));
    }

    false
}

fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> bool {
    let started = Instant::now();

    while started.elapsed() < timeout {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => thread::sleep(Duration::from_millis(100)),
            Err(_) => return true,
        }
    }

    false
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}
