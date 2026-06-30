# 路线图

本文档描述 `ferris-agent-bridge` 在早期 `0.x` 开发阶段的计划方向。它是规划指南，不是兼容性承诺。

项目应保持 Rust-first 和 local-first：

- 本地 agent 进程运行在用户机器上。
- 凭证、文件、session state 和 workspace policy 保持在本地。
- Chat platforms 与 agent CLIs 通过明确的 adapter 边界连接。

## 版本策略

`0.0.x` 线上的 patch release 应限制在发布清理、metadata、文档和小范围 CLI polish。

功能开发从 `0.1.0` 开始。每个 minor version 应有一个清晰的用户可见里程碑，并配套足够测试，避免下个里程碑重建上个里程碑的基础。

## 开发流程

Roadmap 里程碑应在聚焦的 feature branch 上开发。分支命名、合并策略、版本更新、changelog 更新和 release tagging 记录在 [CONTRIBUTING.zh.md](../CONTRIBUTING.zh.md)。

## 实现顺序

实现应从本地服务边界开始，然后向外推进：

- 先构建 daemon lifecycle。
- 加入 core runtime 和 session model。
- 用一个受控 adapter 证明 agent 侧。
- 在 runtime 和 agent 侧稳定后，加入第一个 chat platform adapter。

这样早期里程碑可以保持独立，不依赖任何单一 chat platform SDK。对 Lark / Feishu 来说，需要时可以用 Rust 实现 OpenAPI HTTP client，而 event transport 留到后续 IM adapter 阶段处理。

Adapter contracts 应基于能力，而不是 SDK。未来的官方 SDK 或 channel API 应该可以在这些 contract 后替换，而不改变 core runtime。

公共接口应描述 bridge 能力，而不是 provider 内部结构。Provider API clients、event transports 和 event dispatchers 是具体平台实现可能用到的构件，但不是每个 IM adapter 的必需形状。未来 adapter 可以直接包装官方 channel SDK，组合独立的 REST 和 event clients，或者自行补齐缺失的一侧。WebSocket 和 webhook 应是 transport 实现，而不是固定架构选择。

## 0.1.0 - Local Daemon Foundation

目标：

提供可用的本地服务基础和基本 lifecycle commands。这个版本应证明项目可以安全管理本地后台进程，然后再连接 chat platforms 或真实 agent CLIs。

设计说明：[0.1 Daemon 生命周期设计](design/0.1-daemon-lifecycle.zh.md)。

包含：

- `start`、`stop`、`status` CLI commands。
- 便于调试的 foreground 或 development mode。
- 本地状态的 runtime directory layout。
- 防止重复服务实例的 atomic lock handling。
- Stale lock detection 和 PID ownership validation。
- 基本 service state reporting。
- 适合本地排障的 minimal logging。
- command parsing 和 daemon lifecycle behavior 的测试。

不包含：

- Chat platform integration。
- Lark / Feishu OpenAPI 或 channel integration。
- 真实 agent CLI execution。
- 跨 chat conversations 的 session continuity。
- Streaming replies。

验收标准：

- `ferris-agent-bridge start` 启动本地服务或 foreground runtime。
- `ferris-agent-bridge status` 报告服务是否运行。
- `ferris-agent-bridge stop` 干净停止服务。
- 重复 start 不会创建多个 active services。
- 并发 start 会被 lock 串行化。
- Stale locks 和 PID reuse 能被检测出来，且不会停止无关进程。
- Shutdown 有 graceful timeout 和清晰 failure path。
- 实现有聚焦测试覆盖，并以 macOS 作为第一个目标平台工作。

## 0.2.0 - Runtime and Session Foundation

目标：

引入后续 adapters 所需的内部 runtime model。

包含：

- Profile 或 config loading。
- Runtime state storage。
- Session identity 和 continuity model。
- 基本 message queue 和 run lifecycle types。
- 面向 normalized IM events 与 agent runs 的具体 runtime orchestrator。
- 用于处理重复投递的 inbound event ledger。
- Run state machine 和 startup recovery strategy。
- Ack-after-persist 与 outbound outbox contracts。
- Core `message` 和 `event` domain models。
- 最小 `ImAdapter` 和 `AgentAdapter` capability boundaries。
- Workspace policy skeleton。
- Access policy skeleton。
- Structured internal events。

不包含：

- 完整 IM adapter support。
- 完整 agent adapter support。
- 宽泛、可替换的 `Runtime` trait。
- Platform-specific permissions 或 card rendering。
- Platform-specific auth、transport、OpenAPI、message 或 event payload types。

验收标准：

- Runtime state 可以安全创建、加载和更新。
- Sessions 与 queued messages 有稳定 identifiers。
- 重复 inbound events 不会创建重复 runs。
- Runtime restart 可以把 pending、running 或 failed runs 恢复为显式状态。
- Event acknowledgement 只在最小 durable state 记录之后发生。
- Workspace 和 access policy decisions 显式且可测试。
- `0.1.0` 的 daemon foundation 保持完整。

## 0.3.0 - First Agent Adapter

目标：

用一个受控 adapter 证明 agent 侧 bridge 能力。

包含：

- 以 mock 或 echo adapter 作为第一个稳定 adapter target。
- 用 controlled fixture CLI 验证真实 process boundary。
- 面向后续真实 CLI adapters 的 process spawning abstraction。
- 基本 stdout、stderr、exit-code、timeout 和 cancellation handling。
- 内部 `AgentEvent` stream shape。
- Cancellation 和 timeout behavior。
- 使用 fake processes 或 controlled adapters 的测试。

不包含：

- 对多个 coding agents 的广泛支持。
- 完整 Claude、Codex 或 Trae compatibility。
- Chat platform integration。

验收标准：

- Runtime 可以启动 adapter run 并接收 structured events。
- Fixture CLI 能证明 spawning、stdout/stderr capture、exit-code mapping、timeout 和 cancellation。
- Cancellation 和 timeout behavior 是确定性的。
- Adapter output 可以映射到通用内部 event model。

## 0.4.0 - First Chat Platform Integration

目标：

完成第一条最小端到端 bridge 路径。

包含：

- 一个初始 chat platform adapter。
- 面向 inbound events 和 outbound replies 的内部 `ImAdapter` capability boundary。
- 所选平台的 platform-specific auth、event transport、API client、message types 和 event types。
- Incoming message normalization。
- Outgoing reply path。
- 连接 chat events、runtime sessions 和第一个 agent adapter。
- 所选平台的 minimal local configuration。
- 面向远程触发本地 runs 的 minimum safety envelope。

不包含：

- 广泛 multi-platform support。
- Rich interactive cards。
- Advanced attachment handling。
- 超出 minimum safety envelope 的 production-grade permission policy。

验收标准：

- 一条 chat message 可以触发本地 runtime run。
- Runtime 可以调用第一个 agent adapter。
- Reply 可以发送回 chat platform。
- 未知 chats 和 users 默认拒绝。
- 本地执行前强制检查 workspace allowlists 和 agent command 或 profile allowlists。
- Policy decisions 会记录日志，便于本地排障。
- 失败会产生清晰 local logs 和 user-facing errors。

## 后续里程碑

第一条端到端路径稳定后，再规划未来工作。

可能方向：

- Additional agent adapters。
- Additional chat platform adapters。
- 可在官方 SDK 可用时包装它们的 alternative platform adapter implementations。
- Attachment handling。
- Streaming updates。
- Workspace allowlists 和更强 access policy。
- Cross-platform service management。
- Installer 和 release automation。
