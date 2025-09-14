# RDF Fusion Examples

This crate contains examples for using and extending RDF Fusion.

## Running Examples

To run an example, use the cargo run command, such as:

```bash
git clone https://github.com/tobixdev/rdf-fusion
cd rdf-fusion
cd examples
cargo run --example use-store # Run the `use-store` example. Replace `use-store` with the name of the example.
```

## Basic Usage

* **[use-store](./examples/use_store.rs)**: Using RDF Fusion via an adaption of [Oxigraph](https://github.com/oxigraph/oxigraph)'s 
  Store API. 
* **[query-store](./examples/query_store.rs)**: Send a basic query against the store.

## Advanced Usage

* **[plan-builder](./examples/plan_builder.rs)**: Use RDF Fusion's plan builder to create a custom query plan.

