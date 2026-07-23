# artel — agent guidance

## Tests

This workspace uses `cargo nextest` as the canonical test runner. **Do not invoke `cargo test`** for routine workspace runs — it runs serially across binaries and is significantly slower than nextest.

- `make test` — Tier A + B (default profile; filters `_n0` tests) with a coverage summary as a side effect (via `cargo llvm-cov nextest`; falls back to plain nextest if cargo-llvm-cov is missing). What you should run after a change. Instrumentation overhead on test runtime is ~zero (measured 2026-07-23); the cost is the separate instrumented build under `target/llvm-cov-target`.
- `make test-n0` — Tier C (real n0). Slow; opt-in.
- `make ci-local` — fmt + clippy + full pyramid.
- Doctests still go through `cargo test --workspace --doc --all-features` — nextest doesn't execute doctests; the Makefile pairs the two.
- Test config: `.config/nextest.toml`. Tier C functions are suffixed `_n0`.
- Per memory: redirect long test output to files; don't tail-eyeball.

For one-off targeted runs, prefer `cargo nextest run --package <crate> --test <bin>` over `cargo test`.

## Lints / fmt / docs / coverage

- `make fmt` — `cargo fmt --all --check`.
- `make clippy` — both feature modes (default and `--all-features`); `-D warnings`.
- `make doc` — rustdoc both feature modes; catches broken intra-doc links.
- Coverage rides `make test` (see above); `make coverage-html` produces the HTML report at `target/llvm-cov/html/index.html`. Tier C skipped from coverage — see Makefile comment. Requires `cargo install cargo-llvm-cov` once (plus `rustup component add llvm-tools-preview`).
- `make ci-local` — fmt + clippy + doc + tests + n0.

**Pushes are gated by a pre-push hook** (`.githooks/pre-push`: fmt + clippy + doc) — run `make hooks` once per clone to activate it. A CI rerun costs ~10 minutes; `make doc` in particular catches rustdoc `-D warnings` failures (e.g. private-intra-doc-links) that fmt/clippy/tests all miss. Tests are not in the hook (too slow for WIP pushes) — run `make test` before marking a PR ready. Never bypass with `--no-verify` unless the user asks.

## Plans / brainstorms

- Brainstorms: `docs/brainstorms/<date>-<topic>.md`
- Plans: `docs/plans/<date>-<topic>.md`
- Handoff docs (`docs/handoff-*.md`) are local working artifacts — **never** `git add` them (per memory).
