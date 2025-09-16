# RDF Fusion

<p align="center">
  <img src="./logo.png" width="128" alt="RDF Fusion Logo" align="right">
</p>

RDF Fusion is an experimental columnar [SPARQL](https://www.w3.org/TR/sparql11-overview/) engine.
It is based on [Apache DataFusion](https://datafusion.apache.org/), an extensible query engine based
on [Apache Arrow](https://arrow.apache.org/).

A primary goal of RDF Fusion is to preserve the strengths of DataFusion and make them available to the Semantic Web
community.
These strengths include:

- Extensibility: DataFusion features many extension points that we use to implement SPARQL. We expose these extension
  points to RDF Fusion users for developing customized SPARQL dialects. In the future, we would like to provide
  further extension points.
- Performance: DataFusion features a vectorized execution engine that can leverage the capabilities of modern CPUs.
  We track RDF Fusion's performance on [CodSpeed](https://codspeed.io/tobixdev/rdf-fusion) and will provide comparisons
  to other query engines soon.
- Boring Architecture: DataFusion implements an "industry-proven" architecture for query planning and query execution.
  We refer to [DataFusion's documentation](https://datafusion.apache.org/contributor-guide/architecture.html) for this
  purpose.

## Using RDF Fusion

RDF Fusion can currently be used in two modes: as a "library" for DataFusion or via a convenient Store API.

### Store API

The `Store` API provides high-level methods for interacting with a triple store (e.g., inserting, querying).
Users that want to *use RDF Fusion's* capability are advised to use this API.
While the `Store` API is similar to [Oxigraph](https://github.com/oxigraph/oxigraph)'s `Store` there is not a full
compatibility.

TODO point to example

### Library Use

Only use RDF Fusion as a "library" for DataFusion and directly interact with DataFusion's APIs.
Users that want to *significantly extend RDF Fusion's* capability are advised to use this API.
Note that limited extension points can also be used via the `Store` API (e.g., not altering SPARQL syntax).

Users can use RDF Fusion's implementation of SPARQL operators directly via DataFusion.
They have full control over the processing of the query and only choose and pick the required parts of RDF Fusion. 

TODO point to example

## Comparison with Other SPARQL Databases

Here is a short comparison with other open-source SPARQL databases.

- [Oxigraph](https://github.com/oxigraph/oxigraph) was a major inspiration for this project, as RDF Fusion started as a
  fork from it. Oxigraph builds on a row-based query engine and cannot be extended with DataFusion's extension points.
  On the other side, Oxigraph is expected to have less overhead for non-CPU-bound queries and is more "battle-tested".
- [Apache Jena](https://jena.apache.org/) is probably the de facto standard for experimenting with custom SPARQL
  dialects. It is implemented in Java and has a row-based query engine.

## Getting Started

You can use `cargo` to interact with the codebase or use [Just](https://github.com/casey/just) to run the pre-defined
commands, also used for continuous integration builds.

```bash
git clone --recursive https://github.com/tobixdev/graphfusion.git # Clone Repository
git submodule update --init # Initialize submodules
just test # Run tests 
```

## Help

Feel free to use [GitHub discussions](https://github.com/tobixdev/graphfusion/discussions) to ask questions or talk
about RdfFusion.
[Bug reports](https://github.com/tobixdev/graphfusion/issues) are also very welcome.

## License

This project is licensed under the Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE.txt) or
http://www.apache.org/licenses/LICENSE-2.0).

As this project started as a fork of [Oxigraph](https://github.com/oxigraph/oxigraph), it still contains some code
from Oxigraph. Oxigraph was originally licensed under Apache 2.0 OR MIT. This means that the Oxigraph portions of
the code can also be used under the MIT license, while all new contributions in this repository are provided only
under Apache 2.0.

The license files of Oxigraph at the moment of the fork can be found in [oxigraph_license](./misc/oxigraph_license).

## Contributing

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in RDF Fusion by you, as
defined in the Apache-2.0 license, shall be licensed as above, without any additional terms or conditions.

## Acknowledgements

The project started as a fork from [Oxigraph](https://github.com/oxigraph/oxigraph), a graph database written in Rust
with a custom SPARQL query engine.
While large portions of the codebase have been written from scratch, there is still code from Oxigraph in this
repository.
