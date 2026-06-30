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
        CliCommand::Start { foreground } => {
            let paths = daemon::DaemonPaths::from_env()?;

            if foreground {
                daemon::start_foreground(&paths)
            } else {
                daemon::start_background(&paths)
            }
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
    Start {
        foreground: bool,
    },
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
        [command, rest @ ..] if command == "start" => parse_start_args(rest),
        [command] if command == "stop" => Ok(CliCommand::Stop),
        [command] if command == "status" => Ok(CliCommand::Status),
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

fn parse_start_args(args: &[String]) -> Result<CliCommand, String> {
    match args {
        [] => Ok(CliCommand::Start { foreground: false }),
        [flag] if flag == "--foreground" => Ok(CliCommand::Start { foreground: true }),
        [flag] if flag == "--help" || flag == "-h" => Ok(CliCommand::Help),
        [unknown, ..] => Err(format!(
            "unknown start option: {unknown}\n\nRun `{NAME} --help` for usage."
        )),
    }
}

fn help_text() -> String {
    format!(
        "{NAME} {VERSION}\n{DESCRIPTION}\n\nUSAGE:\n    {NAME} [OPTIONS]\n    {NAME} <COMMAND>\n\nCOMMANDS:\n    start      Start the local daemon\n    stop       Stop the local daemon\n    status     Show daemon status\n    help       Print help\n\nOPTIONS:\n    -h, --help       Print help\n    -V, --version    Print version\n\nSTART OPTIONS:\n        --foreground    Run the daemon in the foreground\n\nENV:\n    FERRIS_AGENT_BRIDGE_HOME    Override the local runtime directory"
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
    fn parses_daemon_commands() {
        assert_eq!(
            parse_args(vec!["start".to_owned()]).expect("start should parse"),
            CliCommand::Start { foreground: false }
        );
        assert_eq!(
            parse_args(vec!["start".to_owned(), "--foreground".to_owned()])
                .expect("foreground start should parse"),
            CliCommand::Start { foreground: true }
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
