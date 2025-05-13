# RdfFusion

> **⚠️ DISCLAIMER: This project is still heavily under development and is not ready for production use. APIs may change, features may be incomplete, and there might be performance issues or bugs.**


RdfFusion is a graph database implementing the [SPARQL](https://www.w3.org/TR/sparql11-overview/) standard.

The query engine of RdfFusion is based on the [Apache Arrow](https://arrow.apache.org/) columnar format and
[Apache DataFusion](https://datafusion.apache.org/), an extensible query engine based on Arrow. 

## Getting Started

You can use `cargo` to interact with the codebase or use [Just](https://github.com/casey/just) to run the pre-defined
commands, also used for continuous integration builds.

```bash
git clone --recursive https://github.com/tobixdev/graphfusion.git # Clone Repository
git submodule update --init # Initialize submodules
just check # Run checks 
```

## Help

Feel free to use [GitHub discussions](https://github.com/tobixdev/graphfusion/discussions) to ask questions or talk
about RdfFusion.
[Bug reports](https://github.com/tobixdev/graphfusion/issues) are also very welcome.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
  http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  http://opensource.org/licenses/MIT)

at your option.

## Contributing

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in RdfFusion by you, as
defined in the Apache-2.0 license, shall be dually licensed as above, without any additional terms or conditions.

## Acknowledgements

The project started as a fork from [Oxigraph](https://github.com/oxigraph/oxigraph), a graph database written in Rust
with a custom SPARQL query engine.
While large portions of the codebase have been written from scratch, there is still code from Oxigraph in this
repository.
