# artel — top-level developer commands.
#
# Test pyramid (per docs/plans/2026-05-29-faster-cargo-test-plan.md):
#   Tier A — no iroh `Endpoint` bound.        Default profile.  Fast.
#   Tier B — iroh hermetic (DnsPkarrServer +
#            TestingUnreachableRelay).         Default profile.  Medium.
#   Tier C — real n0.                          `n0` / `ci` profiles. Slow.
#
# Tier C tests have test-fn names suffixed `_n0`; the default profile
# filters them out via `not test(/_n0$/)`. See
# docs/diagnosing-flaky-tests.md for the run-until-fail recipe.

.PHONY: test test-n0 test-fallback fmt clippy doc coverage coverage-html ci-local

# Default test target: Tier A + B (no real n0). Fast.
test:
	cargo nextest run --workspace
	cargo test --workspace --doc --all-features

# Real-n0 tests only. Serial within the tier (per nextest profile)
# so a failing iteration's tracing log is a single coherent timeline.
test-n0:
	cargo nextest run --workspace --profile n0

# Fallback target: cargo test instead of nextest. Slower (no
# inter-binary parallelism), but works without nextest installed.
# Doctests run via cargo test in either runner.
test-fallback:
	cargo test --workspace --all-targets
	cargo test --workspace --all-targets --all-features
	cargo test --workspace --doc --all-features

fmt:
	cargo fmt --all --check

# Three feature modes: default, everything on, everything off. The
# no-default-features pass is what catches an iroh-gated item used
# without its cfg — that mode has no test tier of its own, so lint
# coverage here is its only signal. It deliberately omits
# `--all-targets`: pulling test targets into the build graph activates
# each crate's self dev-dependency (`features = ["test-utils"]`), and
# feature unification turns `iroh` back on for the lib — hiding
# exactly the breakage this pass exists to catch.
clippy:
	cargo clippy --workspace --all-targets -- -D warnings
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo clippy --workspace --no-default-features -- -D warnings

# Build rustdoc for the workspace. Mirrors the clippy two-mode shape
# (default + all-features) so a feature-gated link or item failure
# is caught in either build.
doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

# Coverage via cargo-llvm-cov. Requires `cargo install
# cargo-llvm-cov` once (instrumented binaries need llvm-tools and
# cargo-llvm-cov drives them).
#
# `make coverage` prints a per-file summary + workspace total.
# `make coverage-html` writes HTML reports under `target/llvm-cov/html/`.
# Both run Tier A + B (default profile, same filter as `make test`).
# Tier C (`_n0` tests) intentionally skipped: real-n0 traffic doesn't
# trace anything coverage cares about that the hermetic suite misses,
# and we don't want an unreachable relay flake to mask coverage drift.
coverage:
	cargo llvm-cov nextest --workspace --summary-only
	cargo llvm-cov nextest --workspace --summary-only --all-features

coverage-html:
	cargo llvm-cov nextest --workspace --html
	@echo "HTML report: target/llvm-cov/html/index.html"

# What CI runs locally — full pyramid.
ci-local: fmt clippy doc test test-n0
