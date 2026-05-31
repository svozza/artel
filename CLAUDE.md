# artel — agent guidance

## Tests

This workspace uses `cargo nextest` as the canonical test runner. **Do not invoke `cargo test`** for routine workspace runs — it runs serially across binaries and is significantly slower than nextest.

- `make test` — Tier A + B (default profile; filters `_n0` tests). What you should run after a change.
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
- `make coverage` — text summary via `cargo-llvm-cov` + nextest, both feature modes (Tier A + B; Tier C skipped — see Makefile comment). Requires `cargo install cargo-llvm-cov` once.
- `make coverage-html` — same data, HTML report at `target/llvm-cov/html/index.html`.
- `make ci-local` — fmt + clippy + doc + tests + n0 (coverage is opt-in).

## Plans / brainstorms

- Brainstorms: `docs/brainstorms/<date>-<topic>.md`
- Plans: `docs/plans/<date>-<topic>.md`
- Handoff docs (`docs/handoff-*.md`) are local working artifacts — **never** `git add` them (per memory).
