# Contributing to ferris-agent-bridge

Thanks for considering contributing! This document outlines the development workflow and conventions for this project.

## Branch Naming

- `main`: stable branch, always buildable. Direct commits are not allowed.
- `feat/*`: new features (for example, `feat/daemon-foundation`).
- `fix/*`: bug fixes (for example, `fix/start-command-pid`).
- `release/*`: release preparation (optional, for multi-contributor scenarios).

## Commit Messages

Use conventional commits to keep history readable:

- `feat:` new feature
- `fix:` bug fix
- `docs:` documentation changes
- `chore:` maintenance tasks
- `refactor:` code refactoring

Example: `feat: add start command with PID file support`

Keep the subject line short and specific. Add a body only when the change needs extra context.

When a commit body lists multiple details, prefer an unordered list:

```text
docs: add roadmap and contribution workflow

- Document early 0.x milestones and acceptance criteria.
- Add branch naming, merge, versioning, changelog, and release rules.
- Keep roadmap details separate from contribution workflow details.
```

## Merging

- Feature branches are merged into `main` with squash merging.
- The repository is configured to use the PR title and description as the default squash commit message.
- Write the PR title as the final commit title that should appear on `main`.
- The squash commit title should summarize the PR outcome, not each intermediate commit.
- Keep detailed implementation notes in the PR description when needed.
- Always ensure tests pass before merging.

## Crate Layout

This project starts as a single crate. It may evolve into a Cargo workspace when internal boundaries become stable.

Use `crates/` only for code with clear ownership, a focused public API, and independent tests. Prefer modules until those boundaries are proven.

External SDK-style projects should normally stay as external dependencies. For example, a reusable Lark / Feishu channel SDK can be consumed by the Lark IM adapter instead of being copied into this repository.

## Versioning and Changelog

This project follows [Semantic Versioning](https://semver.org/).

- If a `release/*` branch exists: update the `Cargo.toml` version and `CHANGELOG.md` in the `release/*` branch, just before tagging.
- If no `release/*` branch exists, which is the default for single-contributor work: update the `Cargo.toml` version and `CHANGELOG.md` as the final change before opening or merging the PR.

General rule: version information must be finalized before a release tag is created.

## Release Process

1. Complete development on a `feat/*` or `fix/*` branch.
2. Update `Cargo.toml` and `CHANGELOG.md` according to the versioning and changelog rules above.
3. Merge the branch into `main`, if not already merged.
4. Create an annotated tag: `git tag -a vX.Y.Z -m "Release vX.Y.Z: description"`
5. Push the tag: `git push origin vX.Y.Z`
6. Publish to crates.io: `cargo publish`
