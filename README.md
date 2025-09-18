# RDF Fusion

<p align="center">
  <img src="./logo.png" width="128" alt="RDF Fusion Logo" align="right">
</p>

RDF Fusion is an experimental columnar [SPARQL](https://www.w3.org/TR/sparql11-overview/) engine.
It is based on [Apache DataFusion](https://datafusion.apache.org/), an extensible query engine that
uses [Apache Arrow](https://arrow.apache.org/) as its in-memory data format.

This project aims to provide a platform for experimenting with SPARQL and domain-specific dialects.
By building on top of DataFusion, users can interact with other efforts within the Arrow and DataFusion community.
While currently RDF Fusion is still in an early stage, we hope that at some point RDF Fusion can be used as a
standalone SPARQL engine in production.

A primary goal of RDF Fusion is to preserve the strengths of DataFusion and make them available to the Semantic Web
community.
These strengths include:

- Extensibility: DataFusion features many extension points that we use to implement SPARQL.
  We expose these extension points to RDF Fusion users for experimenting with SPARQL and domain-specific dialects.
  In the future, we would like to provide further extension points that are tailored towards SPARQL.
- Performance: DataFusion features a vectorized execution engine that can leverage the capabilities of modern CPUs.
  We track RDF Fusion's performance on [CodSpeed](https://codspeed.io/tobixdev/rdf-fusion) and will provide comparisons
  to other query engines soon in a [separate repository](https://github.com/tobixdev/sparql-bencher/).
- Boring Architecture: DataFusion implements an "industry-proven" architecture for query planning and query execution.
  If logical plans and execution plans are familiar to you, you will feel right at home.
  There is no need to learn a fundamentally different architecture for working SPARQL.
  We refer to [DataFusion's documentation](https://datafusion.apache.org/contributor-guide/architecture.html) for this
  purpose.

## Using RDF Fusion

Documentation for using can be found in the main crate's [README](./lib/rdf-fusion/README.md).
Examples of using RDF Fusion can be found in the [examples](./examples) directory.

## Comparison with Some Other SPARQL Engines

Here is a short comparison with other open-source SPARQL engines.

- [Apache Jena ARQ](https://jena.apache.org/) is a well-known SPARQL engine written in Java.
  It also has a rich set of extension points which have been leveraged in multiple research prototypes.
  Supporting a similar set of SPARQL extension points for columnar query execution is one goal of RDF Fusion.
  We believe that while Jena may remain the go-to platform for experimenting with SPARQL, RDF Fusion has aspects that
  Jena cannot easily provide. Most notably, the integration with the Arrow and DataFusion communities and a columnar
  execution model.
- [Oxigraph](https://github.com/oxigraph/oxigraph) is a relatively new SPARQL engine written in Rust.
  It was a major inspiration for this project, as RDF Fusion started as a fork from it.
  Oxigraph uses a custom-built row-based query engine that has very low overhead and is thus fast for simple
  data retrieval queries.
  However, the extension system is not as extensive as DataFusion's approach and RDF Fusion tends to perform better on
  queries that involve processing large amounts of data.

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

## Acknowledgements

The project started as a fork from [Oxigraph](https://github.com/oxigraph/oxigraph), a graph database written in Rust
with a custom SPARQL query engine.
While large portions of the codebase have been written from scratch, there is still code from Oxigraph in this
repository.
