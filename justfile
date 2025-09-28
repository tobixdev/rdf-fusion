# List available commands
default:
    @just --list

# Install development tools. Assumes Rustup and toolchain is installed.
configure-toolchain:
    rustup component add clippy rustfmt
    cargo install cargo-deny

# Install development tools. Assumes Rustup and toolchain is installed.
configure-toolchain-ci: configure-toolchain
    cargo install cargo-codspeed

# Run all lints (e.g., formatting, clippy)
lint:
    cargo fmt -- --check
    cargo clippy -- -D warnings -D clippy::all
    # cargo deny check

# Run all tests
test:
    cargo test --workspace --exclude rdf-fusion-examples

# Runs all examples to see whether they fail
test-examples:
    cargo run --package rdf-fusion-examples --example custom_function
    cargo run --package rdf-fusion-examples --example custom_storage
    cargo run --package rdf-fusion-examples --example plan_builder
    cargo run --package rdf-fusion-examples --example query_store
    cargo run --package rdf-fusion-examples --example use_store

# Build and check documentation
rustdoc:
    RUSTDOCFLAGS="-D warnings" cargo doc

[working-directory: 'bench']
prepare-benches-tests:
    cargo run --profile test prepare bsbm-explore --num-products 1000 # BSBM use cases share the data
    cargo run --profile test prepare wind-farm --num-turbines 4

[working-directory: 'bench']
prepare-benches:
    cargo run --profile profiling-nonlto prepare bsbm-explore --num-products 10000 # BSBM use cases share the data
    cargo run --profile profiling-nonlto prepare wind-farm --num-turbines 16

# Starts a webserver that can answer SPARQL queries (debug)
serve-dbg:
    cargo run --bin rdf-fusion -- serve --bind 0.0.0.0:7878

# Starts a webserver that can answer SPARQL queries (profiling)
serve:
    RUSTFLAGS="-C target-cpu=native" cargo run --profile profiling --bin rdf-fusion -- serve --bind 0.0.0.0:7878

#
# Releases
#

prepare-release:
    #!/usr/bin/env bash
    if git status --porcelain; then \
        echo "The working directory is not clean. Commit ongoing work before creating a release archive."; \
        exit 1; \
    fi
    git archive --format=tar.gz -o target/rdf-fusion-source-0.1.0.tar.gz HEAD
