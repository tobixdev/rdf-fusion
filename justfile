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

# Run all recipies executed by the CI
ci: lint test rustdoc

# Run all lints (e.g., formatting, clippy)
lint:
    cargo fmt -- --check
    cargo clippy -- -D warnings -D clippy::all
    # cargo deny check

# Run all tests
test:
    cargo test

# Build and check documentation
rustdoc:
    RUSTDOCFLAGS="-D warnings" cargo doc

[working-directory: 'bench']
prepare-benches:
    cargo run --profile profiling-nonlto prepare bsbm-explore --num-products 25000
    cargo run --profile profiling-nonlto prepare bsbm-business-intelligence --num-products 25000
    cargo run --profile profiling-nonlto prepare wind-farm --num-turbines 16

# Starts a webserver that can answer SPARQL queries (debug)
serve-dbg:
    cargo run --bin rdf-fusion -- serve --bind 0.0.0.0:7878

# Starts a webserver that can answer SPARQL queries (profiling)
serve:
    RUSTFLAGS="-C target-cpu=native" cargo run --profile profiling --bin rdf-fusion -- serve --bind 0.0.0.0:7878