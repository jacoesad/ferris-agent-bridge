# 贡献 ferris-agent-bridge

感谢你考虑参与贡献。本文档说明本项目的开发流程和约定。

## 分支命名

- `main`：稳定分支，必须始终可构建。禁止直接提交。
- `feat/*`：新功能，例如 `feat/daemon-foundation`。
- `fix/*`：问题修复，例如 `fix/start-command-pid`。
- `release/*`：发布准备分支，可选，适用于多人协作场景。

## 提交信息

使用 Conventional Commits，让历史保持可读：

- `feat:` 新功能
- `fix:` 问题修复
- `docs:` 文档变更
- `chore:` 维护任务
- `refactor:` 代码重构

示例：`feat: add start command with PID file support`

标题保持简短、具体。只有在变更需要额外上下文时才添加正文。

当提交正文需要列出多个细节时，优先使用无序列表：

```text
docs: add roadmap and contribution workflow

- Document early 0.x milestones and acceptance criteria.
- Add branch naming, merge, versioning, changelog, and release rules.
- Keep roadmap details separate from contribution workflow details.
```

## 合并

- 功能分支通过 squash merging 合并到 `main`。
- 仓库已配置为默认使用 PR 标题和描述作为 squash commit message。
- PR 标题应按最终出现在 `main` 上的提交标题来写。
- Squash commit 标题应总结 PR 结果，而不是逐条复述中间提交。
- 需要详细实现说明时，放在 PR 描述中。
- 合并前必须确认测试通过。

## Crate 布局

项目从单一 crate 开始。当内部边界稳定后，可以演进为 Cargo workspace。

只有当代码具备清晰 ownership、聚焦的 public API、独立测试时，才使用 `crates/`。在这些边界被验证前，优先使用 module。

外部 SDK 风格的项目通常应保持为外部依赖。例如，可复用的 Lark / Feishu channel SDK 应由 Lark IM adapter 依赖，而不是复制进本仓库。

## 版本与 Changelog

本项目遵循 [Semantic Versioning](https://semver.org/)。

- 如果存在 `release/*` 分支：在 `release/*` 分支中更新 `Cargo.toml` 版本和 `CHANGELOG.md`，然后再打 tag。
- 如果不存在 `release/*` 分支，这也是单人维护时的默认流程：在打开或合并 PR 前，把版本和 `CHANGELOG.md` 作为最后一项变更更新。

通用规则：创建发布 tag 前，必须先确定版本信息。

## 发布流程

发布应从最新 `main` 切短生命周期的 `release/<version>` 分支准备。

Release PR 应只包含发布准备变更：

- 更新 `Cargo.toml` 和 `Cargo.lock` 版本
- 更新 `CHANGELOG.md` 或 release notes
- 发布所需的小范围 package metadata 或 README 修正

Release PR 应运行常规 CI，并完成发布 dry-run 验证，例如 `cargo package` 和 `cargo publish --dry-run`。

Release PR 合并回 `main` 后，应从合并后的 `main` commit 打 tag 并发布。不要在 release 分支合并前从 release 分支打 tag 或发布。

Annotated tag message 保持简短，例如 `Release v0.1.0`。Release highlights、链接和迁移说明应放在 GitHub Release 中。

当前手动发布流程：

1. 从最新 `main` 切短生命周期的 `release/<version>` 分支。
2. 进行仅限发布准备的变更，例如版本、metadata、README 或 release notes 更新。
3. 打开 release PR，并等待 CI 和 release dry-run checks 通过。
4. 将 release PR 合并回 `main`。
5. 将本地 `main` 更新到合并后的 commit。
6. 在合并后的 commit 上运行 `cargo publish --dry-run` 验证。
7. 创建并推送 annotated tag，例如 `v0.1.0`。
8. 从已打 tag 的 `main` commit 运行 `cargo publish`。
9. 确认 crate version 已在 crates.io 可见。
10. 基于已推送的 tag 创建 GitHub Release，并使用匹配的 `CHANGELOG.md` 小节作为 release notes。
11. 当 release 分支不再有用时删除它。

后续可以加入 trusted publishing 和 tag-triggered release automation。
