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

.PHONY: test test-n0 test-fallback fmt clippy ci-local

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

clippy:
	cargo clippy --workspace --all-targets -- -D warnings
	cargo clippy --workspace --all-targets --all-features -- -D warnings

# What CI runs locally — full pyramid.
ci-local: fmt clippy test test-n0
