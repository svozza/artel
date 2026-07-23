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

.PHONY: test test-n0 test-fallback fmt clippy doc coverage-html ci-local hooks

# One-time setup: point git at the versioned hooks dir so the
# pre-push gate (fmt + clippy + doc) runs before every push.
hooks:
	git config core.hooksPath .githooks
	@echo "core.hooksPath -> .githooks (pre-push runs fmt + clippy + doc)"

# Default test target: Tier A + B (no real n0), with line/region
# coverage as a side effect (vitest-style: one run, coverage for
# free). Instrumentation overhead on test runtime is ~zero (measured
# 2026-07-23: cov ≈ plain on macOS and Linux, isolated and full
# suite); the cost is a separate instrumented build under
# `target/llvm-cov-target` (~3 min warm, ~10 min cold). Falls back to
# plain nextest when cargo-llvm-cov isn't installed
# (`cargo install cargo-llvm-cov` + `rustup component add
# llvm-tools-preview` to get coverage).
#
# Doctests aren't instrumented by llvm-cov on stable — they still run
# (uninstrumented) via cargo test below; their coverage isn't counted.
test:
	@if command -v cargo-llvm-cov >/dev/null 2>&1; then \
		cargo llvm-cov nextest --workspace --summary-only; \
	else \
		echo "cargo-llvm-cov not installed — running without coverage"; \
		cargo nextest run --workspace; \
	fi
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

# HTML coverage report from the same instrumented run shape as
# `make test`. Written under `target/llvm-cov/html/`. Runs Tier A + B
# (default profile). Tier C (`_n0` tests) intentionally skipped:
# real-n0 traffic doesn't trace anything coverage cares about that
# the hermetic suite misses, and we don't want an unreachable relay
# flake to mask coverage drift.
coverage-html:
	cargo llvm-cov nextest --workspace --html
	@echo "HTML report: target/llvm-cov/html/index.html"

# What CI runs locally — full pyramid.
ci-local: fmt clippy doc test test-n0
