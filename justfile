# List available commands
default:
    @just --list

# Install development tools and dependencies
setup:
    rustup component add clippy rustfmt
    cargo install cargo-deny

# Run all checks (static & tests)
check: check-static test rustdoc

# Run all static checks (e.g., formatting, clippy)
check-static: clippy deny
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
    cargo deny check

# Build and check documentation
rustdoc:
    RUSTDOCFLAGS="-D warnings" cargo doc