# RDF Fusion Examples

This crate contains examples for using and extending RDF Fusion.

## Running Examples

To run an example, use the cargo run command, such as:

```bash
git clone https://github.com/tobixdev/rdf-fusion
cd rdf-fusion
cd examples
cargo run --example use_store # Run the `use_store` example. Replace `use_store` with the name of the example.
```

## Basic Usage

* **[use-store](./examples/use_store.rs)**: Using RDF Fusion via an adaption
  of [Oxigraph](https://github.com/oxigraph/oxigraph)'s Store API.
* **[query-store](./examples/query_store.rs)**: Send a basic query against the store.
* **[custom-function](./examples/custom_function.rs)**: Register a custom scalar function.

## Advanced Usage

* **[custom-storage](./examples/custom_storage.rs)**: Use a custom storage implementation.
* **[plan-builder](./examples/plan_builder.rs)**: Use RDF Fusion's plan builder to create a custom query plan.

