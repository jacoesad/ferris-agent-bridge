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
    let args = args.into_iter().collect::<Vec<_>>();

    match args.as_slice() {
        [] => Ok(format!(
            "{NAME} {VERSION}\nEarly development: the core runtime and adapters are not implemented yet.\n\nRun `{NAME} --help` for available options."
        )),
        [flag] if flag == "--help" || flag == "-h" => Ok(help_text()),
        [flag] if flag == "--version" || flag == "-V" => Ok(format!("{NAME} {VERSION}")),
        [unknown, ..] => Err(format!(
            "unknown argument: {unknown}\n\nRun `{NAME} --help` for usage."
        )),
    }
}

fn help_text() -> String {
    format!(
        "{NAME} {VERSION}\n{DESCRIPTION}\n\nUSAGE:\n    {NAME} [OPTIONS]\n\nOPTIONS:\n    -h, --help       Print help\n    -V, --version    Print version"
    )
}

#[cfg(test)]
mod tests {
    use super::run;

    #[test]
    fn prints_help() {
        let output = run(["--help".to_owned()]).expect("help should succeed");
        assert!(output.contains("USAGE:"));
        assert!(output.contains("--version"));
    }

    #[test]
    fn prints_version() {
        let output = run(["--version".to_owned()]).expect("version should succeed");
        assert!(output.starts_with("ferris-agent-bridge "));
    }

    #[test]
    fn rejects_unknown_arguments() {
        let err = run(["start".to_owned()]).expect_err("unknown command should fail");
        assert!(err.contains("unknown argument: start"));
    }
}
