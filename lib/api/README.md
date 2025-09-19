# RDF Fusion API

This crate provides an interface to the [RDF Fusion](../rdf-fusion) extension API, enabling the development of
extensions for the SPARQL engine.
Our goal is to keep this crate more stable than the main RDF Fusion crate.

Currently, it allows you to implement:

- Custom SPARQL function registries
- Custom storage layers

While we aim to maintain a stable API, the crate is still evolving, and some APIs may change frequently.
If you are building custom extensions, we welcome your feedback on how to improve this crate and make extension
development easier.