# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2026-07-04

Runtime foundations release.

### Added

- Added versioned runtime configuration with profile, runtime file names, and `SecretInput` placeholders for future secret sources.
- Added runtime state storage with shared atomic JSON persistence and private Unix/macOS permissions for newly created state files and missing parent directories.
- Added stable session identity and continuity models.
- Added platform-neutral `Message` and `Event` domain models.
- Added structured log events with session/event context and secret-value redaction for structured fields and inline values.
- Added runtime error classification into fatal, recoverable, and user-visible classes.
- Added focused tests for config, state, session, message, event, logging, and error behavior.

### Changed

- Restructured the roadmap around current status, architecture boundaries, version policy, and PR-sized milestone bullets.
- Aligned release guidance so milestone releases use dedicated `release/<version>` branches and release PRs.
- Clarified the `0.2.0` runtime foundation boundary and moved runtime orchestration work to the `0.3.0` milestone.
- Updated README status to distinguish the published daemon foundation from the runtime foundation release.

## [0.1.0] - 2026-07-01

Local daemon foundation release.

### Added

- Added local daemon lifecycle commands: `run`, `start`, `status`, and `stop`.
- Added a private local runtime directory with daemon lock, state, stop request, and log files.
- Added foreground daemon mode for development and debugging.
- Added macOS daemon lifecycle integration tests for start/status/stop, duplicate starts, concurrent starts, foreground stop, file permissions, and invalid-state fallback stop.
- Added daemon lifecycle design documentation in English and Chinese.
- Added required macOS CI checks and Linux/Windows compatibility build checks.

### Changed

- Clarified the early `0.x` roadmap around the local daemon foundation milestone.
- Documented current platform support as macOS behavior validation with Linux and Windows build-only compatibility gates.

## [0.0.1] - 2026-06-26

Initial early-development release.

### Added

- Added minimal Rust CLI output for project metadata.
- Added `--help` and `--version` support.
- Added crates.io package metadata.
- Added initial README and architecture notes.
- Added dual MIT OR Apache-2.0 licensing.
