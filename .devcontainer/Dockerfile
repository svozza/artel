# artel devcontainer image.
#
# Based on the official devcontainers Rust image (Debian bookworm),
# which ships rustup plus the build-essential / clang toolchain that
# the `ring` crate (pulled in via iroh's `tls-ring` feature) needs to
# compile. We don't rely on the toolchain version baked into the tag —
# the `rustup update` below pulls current stable at build time.
FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

# Pin to current stable rather than whatever the base-image tag last
# baked in (it can lag true latest stable by weeks). The workspace is
# edition 2024 with MSRV rust-version = "1.95"; this guarantees a
# toolchain that satisfies both. Components added in the same step:
#   - rustfmt / clippy: `make fmt` / `make clippy`.
#   - llvm-tools-preview: instrumented binaries for `make coverage`
#     (cargo-llvm-cov drives the llvm-tools).
RUN rustup update stable \
    && rustup default stable \
    && rustup component add rustfmt clippy llvm-tools-preview

# Install the workspace's canonical cargo tooling from prebuilt
# binaries via cargo-binstall (far faster than `cargo install`,
# which would compile them from source in the image build):
#   - cargo-nextest:  the canonical test runner (`make test`).
#   - cargo-llvm-cov: coverage (`make coverage` / `coverage-html`).
RUN curl -L --proto '=https' --tlsv1.2 -sSf \
        https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash \
    && cargo binstall -y cargo-nextest cargo-llvm-cov \
    && rm -rf /usr/local/cargo/registry

# Claude Code CLI, so agent sessions can run inside the container
# (against the container's toolchain, sandboxed from the host). The
# native installer is per-user: run it as vscode (the remoteUser) and
# symlink into /usr/local/bin so it's on PATH for any user/shell.
RUN su vscode -c 'curl -fsSL https://claude.ai/install.sh | bash' \
    && ln -sf /home/vscode/.local/bin/claude /usr/local/bin/claude
