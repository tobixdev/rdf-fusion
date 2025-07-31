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

# Runs micro benchmarks in the code base (criterion benchmarks).
microbench:
    cargo bench

# Runs micro benchmarks in the code base (criterion benchmarks) and creates a flamegraph.
microbench-flamegraph bench:
    cargo flamegraph --bench {{bench}} -- --bench

# Starts a webserver that can answer SPARQL queries (debug)
serve-dbg:
    cargo run --bin rdf-fusion -- serve --bind 0.0.0.0:7878

# Starts a webserver that can answer SPARQL queries (profiling)
serve:
    RUSTFLAGS="-C target-cpu=native" cargo run --profile profiling --bin rdf-fusion -- serve --bind 0.0.0.0:7878