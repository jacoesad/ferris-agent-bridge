mod daemon;

use std::process::ExitCode;

const NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");
const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");

fn main() -> ExitCode {
    match run(std::env::args().skip(1)) {
        Ok(output) => {
            println!("{output}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: impl IntoIterator<Item = String>) -> Result<String, String> {
    match parse_args(args.into_iter().collect::<Vec<_>>())? {
        CliCommand::About => Ok(format!(
            "{NAME} {VERSION}\nLocal daemon lifecycle commands are available.\n\nRun `{NAME} --help` for available options."
        )),
        CliCommand::Help => Ok(help_text()),
        CliCommand::Version => Ok(format!("{NAME} {VERSION}")),
        CliCommand::Run => {
            let paths = daemon::DaemonPaths::from_env()?;
            daemon::run_foreground(&paths)
        }
        CliCommand::Start => {
            let paths = daemon::DaemonPaths::from_env()?;
            daemon::start_background(&paths)
        }
        CliCommand::Stop => {
            let paths = daemon::DaemonPaths::from_env()?;
            daemon::stop(&paths)
        }
        CliCommand::Status => {
            let paths = daemon::DaemonPaths::from_env()?;
            Ok(daemon::status_text(&paths))
        }
        CliCommand::InternalDaemon {
            token,
            starter_pid,
            foreground,
        } => daemon::run_daemon_from_env(token, starter_pid, foreground),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CliCommand {
    About,
    Help,
    Version,
    Run,
    Start,
    Stop,
    Status,
    InternalDaemon {
        token: String,
        starter_pid: u32,
        foreground: bool,
    },
}

fn parse_args(args: Vec<String>) -> Result<CliCommand, String> {
    match args.as_slice() {
        [] => Ok(CliCommand::About),
        [flag] if flag == "--help" || flag == "-h" || flag == "help" => Ok(CliCommand::Help),
        [flag] if flag == "--version" || flag == "-V" => Ok(CliCommand::Version),
        [command, rest @ ..] if command == "run" => parse_no_args(command, rest, CliCommand::Run),
        [command, rest @ ..] if command == "start" => {
            parse_no_args(command, rest, CliCommand::Start)
        }
        [command, rest @ ..] if command == "stop" => parse_no_args(command, rest, CliCommand::Stop),
        [command, rest @ ..] if command == "status" => {
            parse_no_args(command, rest, CliCommand::Status)
        }
        [command, token_flag, token, starter_flag, starter_pid]
            if command == "__daemon"
                && token_flag == "--token"
                && starter_flag == "--starter-pid" =>
        {
            Ok(CliCommand::InternalDaemon {
                token: token.to_owned(),
                starter_pid: parse_starter_pid(starter_pid)?,
                foreground: false,
            })
        }
        [
            command,
            token_flag,
            token,
            starter_flag,
            starter_pid,
            foreground,
        ] if command == "__daemon"
            && token_flag == "--token"
            && starter_flag == "--starter-pid"
            && foreground == "--foreground" =>
        {
            Ok(CliCommand::InternalDaemon {
                token: token.to_owned(),
                starter_pid: parse_starter_pid(starter_pid)?,
                foreground: true,
            })
        }
        [command, ..] if command == "__daemon" => {
            Err("invalid internal daemon invocation".to_owned())
        }
        [unknown, ..] => Err(format!(
            "unknown command: {unknown}\n\nRun `{NAME} --help` for usage."
        )),
    }
}

fn parse_starter_pid(value: &str) -> Result<u32, String> {
    value
        .parse::<u32>()
        .map_err(|err| format!("invalid internal daemon starter pid `{value}`: {err}"))
}

fn parse_no_args(command: &str, args: &[String], parsed: CliCommand) -> Result<CliCommand, String> {
    match args {
        [] => Ok(parsed),
        [unexpected, ..] => Err(format!(
            "unexpected {command} argument: {unexpected}\n\nRun `{NAME} --help` for usage."
        )),
    }
}

fn help_text() -> String {
    format!(
        "{NAME} {VERSION}\n{DESCRIPTION}\n\nUSAGE:\n    {NAME} [OPTIONS]\n    {NAME} <COMMAND>\n\nCOMMANDS:\n    run        Run the local daemon in the foreground\n    start      Start the local daemon in the background\n    stop       Stop the local daemon\n    status     Show daemon status\n    help       Print help\n\nOPTIONS:\n    -h, --help       Print help\n    -V, --version    Print version\n\nENV:\n    FERRIS_AGENT_BRIDGE_HOME    Override the local runtime directory"
    )
}

#[cfg(test)]
mod tests {
    use super::{CliCommand, parse_args, run};

    #[test]
    fn prints_help() {
        let output = run(["--help".to_owned()]).expect("help should succeed");
        assert!(output.contains("USAGE:"));
        assert!(output.contains("--version"));
        assert!(output.contains("run"));
        assert!(output.contains("start"));
    }

    #[test]
    fn prints_version() {
        let output = run(["--version".to_owned()]).expect("version should succeed");
        assert!(output.starts_with("ferris-agent-bridge "));
    }

    #[test]
    fn rejects_unknown_arguments() {
        let err = run(["unknown".to_owned()]).expect_err("unknown command should fail");
        assert!(err.contains("unknown command: unknown"));
    }

    #[test]
    fn rejects_trailing_command_arguments() {
        let stop_err =
            parse_args(vec!["stop".to_owned(), "now".to_owned()]).expect_err("stop arg fails");
        assert!(stop_err.contains("unexpected stop argument: now"));

        let status_err =
            parse_args(vec!["status".to_owned(), "json".to_owned()]).expect_err("status arg fails");
        assert!(status_err.contains("unexpected status argument: json"));

        let run_err =
            parse_args(vec!["run".to_owned(), "now".to_owned()]).expect_err("run extra arg fails");
        assert!(run_err.contains("unexpected run argument: now"));

        let start_err = parse_args(vec!["start".to_owned(), "--foreground".to_owned()])
            .expect_err("start arg fails");
        assert!(start_err.contains("unexpected start argument: --foreground"));
    }

    #[test]
    fn parses_daemon_commands() {
        assert_eq!(
            parse_args(vec!["run".to_owned()]).expect("run should parse"),
            CliCommand::Run
        );
        assert_eq!(
            parse_args(vec!["start".to_owned()]).expect("start should parse"),
            CliCommand::Start
        );
        assert_eq!(
            parse_args(vec!["stop".to_owned()]).expect("stop should parse"),
            CliCommand::Stop
        );
        assert_eq!(
            parse_args(vec!["status".to_owned()]).expect("status should parse"),
            CliCommand::Status
        );
        assert_eq!(
            parse_args(vec![
                "__daemon".to_owned(),
                "--token".to_owned(),
                "token".to_owned(),
                "--starter-pid".to_owned(),
                "42".to_owned(),
            ])
            .expect("internal daemon should parse"),
            CliCommand::InternalDaemon {
                token: "token".to_owned(),
                starter_pid: 42,
                foreground: false,
            }
        );
    }
}
