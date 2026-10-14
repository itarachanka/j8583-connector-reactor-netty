# J8583 Connector AI Agent Instructions

This file provides guidance for AI coding agents working with the J8583 Connector codebase.

## Project Overview

Boilerplate J8583 Connector project provides a structured starting point for a payment adapter that serves as the critical middleware translation layer for a bank's core acquiring switch or card networks like Visa and Mastercard.

## Prerequisites

- Java 25 (default).
- Gradle 9.7.1 (Gradle wrapper `./gradlew` included; prefer it)
- Git

## Commands

### Build

- Full build (Java 25 default): `./gradlew clean build`
- Run tests: `./gradlew clean check`

### Testing

- Single test class: `./gradlew clean test --tests by.ivanta.connector.J8583ConnectorApplicationTests`
- Single test method: `./gradlew clean test --tests by.ivanta.connector.J8583ConnectorApplicationTests#shouldPublishFinancialRequest`
- ArchUnit rules: `./gradlew clean test --tests by.ivanta.connector.architecture.ArchitectureTest'`,

### Code Quality

- Format code: `./gradlew spotlessApply`
- Check formatting: `./gradlew spotlessCheck`
- Checkstyle config: `tools/checkstyle/checkstyle.xml`

### Documentation

- Live documentation: `./gradlew startDocusaurus`

## Repository Structure

### Supporting directories

- `site/` — Documentation content (Docusaurus site). This is where user-facing docs are written.
- `tools/checkstyle/` — checkstyle and suppressions.

### Key packages in `src/main/java`

- `by.ivanta.connector.config` - Spring Boot configs
- `by.ivanta.connector.j8583` - TCP Client source and codecs
- `by.ivanta.connector.support` - Internal components and utils

## Coding Standards

- **Format Java files with Spotless immediately after editing:** `./gradlew spotlessApply`. Uses google-java-format with AOSP style.
- **Checkstyle:** `tools/checkstyle/checkstyle.xml`. Do not suppress rules; fix the code instead.
- **Logging:** Use parameterized log statements (SLF4J `{}` placeholders), never string concatenation.
- **Use `final`** for variables and fields where applicable.
- **Comments:** Do not add unnecessary comments that restate what the code does. Add comments that explain "the why" where relevant.
- **Reuse existing code.** Before implementing new utilities or abstractions, search for existing ones in the codebase. Prioritize architecture consistency and code reusability.

## Testing Standards

- Add tests for new behavior, covering success, failure, and edge cases.
- Use **JUnit 6** + **AssertJ** assertions.
- Prefer real test implementations over Mockito mocks where possible.
- **Red-green verification:** For bug fixes, verify that new tests actually fail without the fix before confirming they pass with it.
- **Test location** mirrors source structure within each module.

## Commits and PRs

### Commit message format

- Separate cleanup/refactoring from functional changes into distinct commits
- When AI tools were used: add `Generated-by: <Tool Name and Version>` trailer

### Pull request conventions

- Fill out the PR template completely but concisely: describe purpose, change log, testing approach, impact assessment
- Each PR should address exactly one issue
- Ensure `./gradlew clean check` passes before opening a PR
- Always push to your fork, not directly to `j8583-connector-reactor-netty`
- Rebase onto the latest target branch before submitting

### AI-assisted contributions

- Disclose AI usage by checking the AI disclosure checkbox and uncommenting the `Generated-by` line in the PR template
- Add `Generated-by: <Tool Name and Version>` to commit messages
- Never add `Co-Authored-By` with an AI agent as co-author; agents are assistants, not authors
- You must be able to explain the design, code, and tests, debug them, and respond to review feedback substantively
- Reviewer-ready quality bar: the author owns PR quality. PRs that look AI-generated without author refinement (walls of unreviewed prose, scaffolding without behaviour, tests that do not exercise the change, padded commit messages) will be closed without review

## Code Review Guidelines

When reviewing a PR or diff against this repo:

- Look for opportunities to simplify the code, scoped to the diff itself (not pre-existing code outside the change).
- Flag comments that are obvious (restate what the code already says) or overly verbose.
- In test code, look for potential flakiness — e.g. `Thread.sleep` used outside a retry/poll loop, or similar timing-dependent, non-deterministic patterns.
- For changes to configuration options, check if the corresponding documentation has been regenerated (this should be covered by tests but flagging it earlier speeds up development)

## Boundaries

### Ask first

- New dependencies
- Changes that could impact performance on hot paths (per-message processing)

### Never

- Commit secrets, credentials, or tokens
- Push directly to `j8583-connector-reactor-netty`; always work from your fork
- Mix unrelated changes into one PR
- Edit generated files by hand when a generation workflow exists
- Add `Co-Authored-By` with an AI agent as co-author in commit messages; AI agents are assistants, not authors. Use `Generated-by: <Tool Name and Version>` instead.
- Suppress or bypass checkstyle rules (no `CHECKSTYLE:ON`/`CHECKSTYLE:OFF` comments, no adding entries to `tools/checkstyle/suppressions.xml`, no `@SuppressWarnings`). Fix the code to satisfy checkstyle instead.
- Use destructive git operations unless explicitly requested

## References

- [README.md](README.md) — Build instructions and project overview
