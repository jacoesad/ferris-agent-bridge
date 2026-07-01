# ferris-agent-bridge

[English](README.md) | [简体中文](README.zh.md)

面向本地 AI agents 与 chat platforms 的 Rust-first bridge。

`ferris-agent-bridge` 是一个早期、从零开始实现的项目，用于从聊天平台触发本地 agent CLI，同时让执行留在用户自己的机器上。它计划作为一个由 CLI 管理的本地 relay service 运行。

## 状态

🚧 早期开发中。已发布的 `0.1.0` 版本提供本地 daemon lifecycle commands，runtime 和 adapter 集成会在后续阶段推进。

## 目标

- 通过可插拔 adapters 连接 chat platforms 与本地 agent CLIs。
- 让 agent 执行、凭证、文件和 workspace state 保持在本地。
- 提供带 `start`、`stop`、`status` 命令的持久本地 daemon/service。
- 支持 session continuity、message queueing、attachments、access policy 和 workspace policy。
- 在加入平台特定 adapters 前，先构建小而可测试的 Rust core。

## 初始范围

第一个实现目标是一条最小端到端路径：

```text
chat message -> IM adapter -> core runtime -> agent adapter -> local CLI -> chat reply
```

设计上应保持 IM adapters、agent adapters 和 runtime policy 分离，这样新增平台和 agent 时不需要重写 core。

架构说明见 [docs/architecture.zh.md](docs/architecture.zh.md)，早期 `0.x` 计划见 [docs/roadmap.zh.md](docs/roadmap.zh.md)。

## 使用

当前 binary 聚焦本地 daemon lifecycle commands：

```bash
cargo run -- --help
cargo run -- --version
cargo run -- run
cargo run -- start
cargo run -- status
cargo run -- stop
```

可以通过 `FERRIS_AGENT_BRIDGE_HOME` 覆盖默认 runtime 目录 `~/.ferris-agent-bridge`。

`run` 会把 daemon loop 留在前台，方便开发和调试。
`start` 当前会直接启动本地后台 daemon；后续加入 OS service 支持时，应继续保留这层语义拆分，由 `start` 管理平台 service wrapper。

## 非目标

- 这不是 cloud-hosted agent runtime。
- 这不是任何现有 bridge implementation 的源码 fork。
- 本项目不应要求用户把本地 agent 凭证迁移到远端服务中。

## 构建与测试

```bash
cargo build
cargo test
```

## 许可证

本项目可按以下任一许可证使用：

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))
