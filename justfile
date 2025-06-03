# List available commands
default:
    @just --list

# Install development tools and dependencies, Assumes Rustup and toolchain is installed
dev-setup:
    rustup component add clippy rustfmt
    cargo install cargo-deny

ci-setup: dev-setup
    cargo install cargo-codspeed

# Run all recipies executed by the CI
ci: lint test rustdoc

# Run all lints (e.g., formatting, clippy)
lint: clippy deny
    cargo fmt -- --check

# Apply code formatting
fmt:
    cargo fmt

# Run clippy lints
clippy:
    cargo clippy -- -D warnings -D clippy::all

# Run all tests
test:
    cargo test

# Run security audit
deny:
    # cargo deny check

# Build and check documentation
rustdoc:
    RUSTDOCFLAGS="-D warnings" cargo doc