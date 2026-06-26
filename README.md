# ferris-agent-bridge

A Rust-first bridge for local AI agents to chat platforms.

`ferris-agent-bridge` is an early-stage, from-scratch project for running local agent CLIs from chat while keeping execution on the user's machine. It is intended to work as a local relay service managed by a CLI.

## Status

🚧 Early development. Version `0.0.1` provides a minimal Rust binary; the core runtime and adapter interfaces are being designed.

## Goals

- Connect chat platforms to local agent CLIs through pluggable adapters.
- Keep agent execution, credentials, files, and workspace state local.
- Provide a durable local daemon/service with `start`, `stop`, and `status` commands.
- Support session continuity, message queueing, attachments, access policy, and workspace policy.
- Start with a small, testable Rust core before adding platform-specific adapters.

## Initial Scope

The first implementation target is a minimal end-to-end path:

```text
chat message -> IM adapter -> core runtime -> agent adapter -> local CLI -> chat reply
```

The design should keep IM adapters, agent adapters, and runtime policy separate so new platforms and agents can be added without rewriting the core.

## Usage

The current binary only exposes project metadata while the runtime is being designed:

```bash
cargo run -- --help
cargo run -- --version
```

## Non-Goals

- This is not a cloud-hosted agent runtime.
- This is not a source-code fork of any existing bridge implementation.
- This project should not require users to move local agent credentials into a remote service.

## Building and Testing

```bash
cargo build
cargo test
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
