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

# Downloads the necessary datasets for the benchmarks
prepare-bench:
    # Clean
    rm -rf ./lib/rdf-fusion/benches-data/
    mkdir -p ./lib/rdf-fusion/benches-data/

    # Download datasets
    curl https://zenodo.org/records/12663333/files/dataset-1000.nt.bz2 -o ./lib/rdf-fusion/benches-data/dataset-1000.nt.bz2
    curl https://zenodo.org/records/12663333/files/dataset-5000.nt.bz2 -o ./lib/rdf-fusion/benches-data/dataset-5000.nt.bz2
    curl https://zenodo.org/records/12663333/files/exploreAndUpdate-1000.csv.bz2 -o ./lib/rdf-fusion/benches-data/exploreAndUpdate-1000.csv.bz2

    # Extract datasets
    bzip2 -d ./lib/rdf-fusion/benches-data/dataset-1000.nt.bz2
    bzip2 -d ./lib/rdf-fusion/benches-data/dataset-5000.nt.bz2
    bzip2 -d ./lib/rdf-fusion/benches-data/exploreAndUpdate-1000.csv.bz2

# Runs benchmarks in the code base.
bench:
    cargo bench

# Runs benchmarks in the code base and creates a flamegraph.
bench-flamegraph:
    cargo bench --bench store