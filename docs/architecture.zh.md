# 架构说明

`ferris-agent-bridge` 计划作为一个本地服务，包含三个主要边界：

```text
IM adapters <-> core runtime <-> agent adapters
```

实现应从中心向外推进。daemon 和 core runtime 不应该依赖 Lark / Feishu、Slack、Discord 或任何具体 agent CLI。平台特有行为应隐藏在 adapter 边界之后。

## Workspace 布局

当边界稳定后，本仓库可以演进为 Cargo workspace。具备清晰 ownership、聚焦 public API、并且可以脱离完整 bridge 应用独立测试的代码，可以移动到 `crates/` 下。

不要只是为了镜像目录结构而拆 crate。一个 crate 应该能被独立理解、测试和版本化。

可能的未来 crate：

```text
crates/ferris-agent-bridge-core
crates/ferris-agent-bridge-runtime
crates/ferris-agent-bridge-daemon
crates/ferris-agent-bridge-agent
crates/ferris-agent-bridge-im
crates/ferris-agent-bridge-im-lark
```

外部 SDK 风格代码应保留在本仓库之外，除非它本身由 bridge 项目拥有。例如，可复用的 Lark / Feishu channel SDK 可以作为外部 crate 被 `ferris-agent-bridge-im-lark` 使用，而不是复制进本 workspace。

## IM Adapters

IM adapters 接收平台特有事件，并将其转换为通用内部事件模型。

示例：

- Lark / Feishu
- Slack
- Discord
- Telegram

Adapter 职责：

- 接收消息、mention、卡片 action、reaction 和附件。
- 将回复、流式更新和最终结果发送回平台。
- 对 core runtime 隐藏平台特有 API 细节。

项目共享抽象应基于能力，而不是仿照某个 provider SDK 的形状。Provider API clients、event transports 和 event dispatchers 是具体平台实现可能用到的细节，但不应该成为所有 IM 平台的必选架构层。其他平台可能提供现成 channel SDK，可能只有 REST API，可能只有 webhook，可能是 gateway connection、long polling，或者完全不同的拆分方式。

公共边界应停留在 bridge 能力层：

```text
Core Runtime
  -> ImAdapter
      -> normalized inbound events
      -> outbound replies and updates
      -> attachment access
      -> platform-specific acknowledgements or cancellation when needed
```

每个 IM adapter 可以选择自己的内部结构。例如 Slack adapter 可能组合 Web API client 与 Socket Mode 或 Events API；Discord adapter 可能组合 REST client 与 Gateway connection。这些实现选择应留在平台 adapter 内部。

事件投递应建模为领域层 transport 能力，而不是固定协议选择。transport trait 应描述 adapter 对交互通道的需求：连接生命周期、输入 raw events、必要时的 delivery acknowledgement、连接状态和 shutdown。WebSocket、webhook、long polling、gateway connections 和官方 channel SDK 都只是这个 transport 边界的具体实现。

当某个 transport 支持显式 delivery acknowledgement 时，adapter 应先请求 runtime 持久化或去重归一化后的 inbound event，然后再 ack 平台 delivery。Foundation layer 提供 persistence primitive，初始 runtime orchestrator 边界通过 `InboundDelivery` 和 `ImAdapter::acknowledge_inbound_delivery` 把这一步接起来：runtime 先记录新 event 或识别 duplicate，然后才调用 adapter acknowledgement。绑定 session 的 inbound message 会在与 ledger record 相同的 state replacement 中进入 durable per-scope queue，因此 acknowledgement 不会抢在 pending work 前完成。重复投递判断使用归一化后的 `EventId`，所以 IM adapter 在把 event 交给 runtime 前，必须按 platform 和 scope 给 provider delivery identifier 加命名空间。如果持久化失败，本次 delivery 必须保持未 ack，让平台按照自身 transport 语义重试。真实 provider transport 仍属于具体 IM adapter 内部。

Queue consumption 由独立的 durable boundary 拥有。`StateStore::claim_message_batch` 只会选择没有 pending、running 或 interrupted run 的 scope，并在同一次原子 state replacement 中创建 pending run、持久化其可恢复的输入消息，再精确移除对应的 bounded queue prefix。Runtime 只有在 replacement 成功后才返回 claim，因此同一 owned process 内的并发 worker 不会拿到相同 scope 或 message batch。

Run startup reconciliation 是另一个由 store 拥有的边界。`StateStore::reconcile_runs_at_startup` 会保留具有 durable input 的 pending run 为可恢复状态，但不返回 execution handoff；它把 running 或缺少 input 的 pending run 转为 non-terminal `interrupted` ownership，并显式报告 failed run 而不执行 retry。Interrupted run 在显式解决为 failed 或 cancelled 前继续排除其 scope，避免新 work 与未解决的 agent-side effects 重叠。

Outbound delivery 使用相反方向的 durable boundary。Runtime 先 claim outbox record，再构造包含稳定 delivery id、normalized scope、message 和 attempt number 的 `OutboundDeliveryAttempt`。`ImAdapter::deliver_outbound_message` 接收这个平台无关的 attempt，并且只有在能够确认 provider 未接受请求时才能把 failure 标记为 retryable；不明确的 transport outcome 保持 uncertain，不能自动重试。Provider request types、幂等机制和 transport 细节留在具体 adapter 内部。Runtime 会先记录 adapter outcome，再调度下一次 attempt。

Outbound startup reconciliation 在同一个 single-daemon 边界下由 store 拥有。`StateStore::reconcile_outbound_deliveries_at_startup` 会把遗留的 `delivering` record 转为 `uncertain`，并报告完整 unresolved id set，不产生另一次 handoff。只有显式“已接受”或“确认未接受”证据才能解决这些 record；exact same-target replay 只用于在 ambiguous write 后确认 durability。同进程 resolution 会被串行化，cross-process writer coordination 仍不属于当前 architecture 阶段。

### Core 与平台模块

Core runtime 模块应定义平台无关的领域模型和行为：

- `message`：内部消息内容、附件、出站回复意图。
- `event`：内部入站事件、run events、归一化用户动作。
- `adapter`：`ImAdapter`、`AgentAdapter` 等能力 trait。
- `runtime`：session、queue、run lifecycle、cancellation 和 adapter orchestration。
- `policy`：跨平台 access、workspace、attachment 和 run policies。

平台特有模块应拥有 provider 细节：

- `auth`：凭证、token refresh、签名和 provider-specific identity。
- `transport`：事件投递生命周期，例如 WebSocket、webhook、long polling、gateway connections 或官方 channel SDK wrapper。
- `openapi` 或 provider API clients：出站 API 调用和 metadata 查询。
- `message`：provider request/response payload、卡片、mention 和 media resources。
- `event`：raw provider event payload 和 provider-specific event names。
- `normalizer`：provider events 到 core events 的转换。
- `outbound`：core outbound intents 到 provider API calls 的转换。

这意味着 `message` 和 `event` 在 core 层与平台层都会存在，但含义不同。Core `message` 和 `event` 类型是稳定的 bridge 领域模型。平台 `message` 和 `event` 类型是 adapter 实现细节，不应泄漏到 core runtime。

### Lark / Feishu 说明

Lark / Feishu 支持包含两个独立部分：

- OpenAPI 调用，用于发送消息、读取 metadata、上传文件、更新交互卡片。
- Event intake，用于接收消息、mention、卡片 action 和其他平台事件。

OpenAPI 侧可以用 Rust 的 HTTP 和 JSON 类型实现。完整 provider SDK 有用，但不是最早里程碑的前置条件。

Event intake 是更大的设计点。Rust 项目应把 channel connector、webhook receiver 或其他 event source 视为 IM adapter 的实现细节。Core runtime 只应看到归一化内部事件。

官方 SDK 或 channel API 应该可以在 adapter 边界后替换。Core runtime 应依赖内部 capability traits 和归一化事件类型，而不是 provider SDK 类型。如果未来出现官方 Rust OpenAPI SDK 或 channel client，应该可以新增 adapter 实现，而不修改 session handling、run lifecycle 或 agent adapters。

推荐内部形状：

```text
Core Runtime
  -> LarkImAdapter
      -> LarkEventTransport
      -> LarkEventSource
      -> LarkOpenApi
      -> LarkEventNormalizer
      -> LarkOutboundSender
```

`LarkImAdapter` 是 Lark 平台 adapter。它应为 Lark-specific 行为提供类似 channel 的边界，但不应克隆完整 channel SDK API，也不应把平台无关的 runtime 行为移动进 adapter。

`LarkEventTransport` 表示事件投递机制这一领域能力。具体实现可以使用长连接 / WebSocket client、webhook receiver、官方 channel SDK 或其他 event stream。Lark adapter 的其他部分不应关心当前启用了哪种 transport 实现。

`LarkEventSource` 是接收 transport 与 dispatch 层之后的 Lark-side normalized events 的推荐 trait 名称。它不应在 trait 边界命名为 `LarkChannelApi`，因为事件可能来自 channel client、webhook receiver、官方 channel API 或其他 event stream 实现。

`LarkOpenApi` 表示出站平台能力，例如发送消息、更新卡片、上传文件和读取 metadata。

`LarkEventNormalizer` 应将 raw Lark events 转换为 core runtime 拥有的内部事件。`LarkOutboundSender` 应包含 Lark-specific 出站格式化和 API 路由，例如消息回复行为、卡片更新、附件下载和 receive-id 处理。

平台无关行为应保留在 core runtime 中，而不是放进 `LarkImAdapter`。这包括 session queueing、run lifecycle、cross-platform access policy、workspace policy、cancellation policy 和 concurrency control。Lark-specific 行为，例如 event type decoding、card action parsing、Lark message resources 和 Lark receive-id rules，应属于 Lark adapter。

替换应按能力独立进行：

- 如果官方 channel API 可用，按需要替换 `LarkEventTransport` 或 `LarkEventSource` 实现。
- 如果官方 OpenAPI SDK 可用，替换 `LarkOpenApi` 实现。
- 如果一个官方 SDK 同时覆盖 event intake 和 OpenAPI calls，一个具体实现可以同时实现两个 trait。

## Core Runtime

Core runtime 拥有不应依赖任何单一 IM 平台或 agent CLI 的行为。

这里的 Runtime 指 bridge 的业务编排器，不是 Rust 任务使用的 async executor。它应该先作为具体组件存在，而不是一开始就抽象成宽泛的 `Runtime` trait。可以在有助于测试或替换时引入更小的依赖 trait，例如 session storage、queues、policy evaluation 和 adapter interfaces。

职责：

- Session identity 和 continuity。
- Pending message queueing 和 batching。
- Access policy。
- Workspace policy。
- Attachment policy。
- Run lifecycle 和 cancellation。
- State storage 和 service locks。
- 在 normalized IM events 与 agent events 之间进行 routing 和 orchestration。

## Runtime State Schema 演进

Runtime state schema version 是内部持久化数据的兼容性标记，与 crate version 和 release version 相互独立。Runtime state 包含 durable ownership、去重、队列、run 和 delivery 信息，因此不能把它当作可随意丢弃的 cache。

- 当序列化表示或持久化语义发生不兼容变化时递增 schema version。Refactor、测试、文档和兼容的字段处理不需要新的 schema version。
- Schema 编号必须单调递增且不得复用，包括只在开发快照中使用过的编号。
- 在 milestone 开发期间，可以暂时在 `main` 保留中间 schema reader，使这些开发快照写入的持久化状态可以向前迁移。
- 在 milestone 发布前，使用独立的 compatibility consolidation PR，仅移除从未由 tagged release 写入的中间 schema migration path。保留受支持 tagged-release schema 和即将发布的最终 schema 的 migration path。该工作应在切 release branch 前完成，使 release PR 仍只包含发布准备变更。
- 对不支持或来自未来版本的 schema 给出明确错误。不得静默删除、降级或重新解释持久化状态。

## Agent Adapters

Agent adapters 运行本地 CLI 工具，并将其输出转换为通用 `AgentEvent` stream。

初始候选：

- Claude Code
- Codex CLI
- Trae CLI

Adapter 职责：

- 构造命令行参数。
- 启动本地进程。
- 传递 prompts 和附件。
- 解析 stdout/stderr。
- 发出 text、tool、usage、done 和 error events。
- 使用清晰的 grace-period policy 停止进程。
- 通过 workspace policy 限制工作目录。
- 使用明确的环境变量 allowlist，并在日志中脱敏敏感值。
- 使用 bounded buffers 和 backpressure 读取 stdout 与 stderr。
- 可预测地终止进程树。
- 将 exit codes 和 signals 映射为结构化 agent errors。

## 第一个里程碑

第一个有用的端到端里程碑应避免宽泛的平台支持。它应该用一个 IM adapter 和一个 agent adapter 证明本地 bridge 的形状：

```text
one chat platform + one local agent CLI + persisted session state
```

只有这条路径稳定后，项目才应该泛化 adapter registration 和 configuration。
