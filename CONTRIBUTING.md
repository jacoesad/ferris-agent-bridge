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

Prepare releases on short `release/<version>` branches cut from the latest `main`.

Release PRs should contain only release preparation changes:

- update `Cargo.toml` and `Cargo.lock` versions
- update `CHANGELOG.md` or release notes
- make small package metadata or README fixes needed for publishing

Release PRs should run the normal CI checks plus release dry-run verification such as `cargo package` and `cargo publish --dry-run`.

After the release PR is merged back to `main`, tag the resulting `main` commit and publish from that commit. Do not tag or publish from the release branch before it is merged.

Keep annotated tag messages short, for example `Release v0.1.0`. Put release highlights, links, and migration notes in the GitHub Release instead.

Current manual release flow:

1. Cut a short `release/<version>` branch from the latest `main`.
2. Make release-only changes, such as version, metadata, README, or release notes updates.
3. Open a release PR and wait for CI and release dry-run checks to pass.
4. Merge the release PR back to `main`.
5. Update local `main` to the merged commit.
6. Verify the merged commit with `cargo publish --dry-run`.
7. Create and push an annotated tag, for example `v0.1.0`.
8. Run `cargo publish` from the tagged `main` commit.
9. Confirm the crate version is visible on crates.io.
10. Create a GitHub Release from the pushed tag, using the matching `CHANGELOG.md` section as the release notes.
11. Delete the release branch when it is no longer useful.

Trusted publishing and tag-triggered release automation may be added later.
