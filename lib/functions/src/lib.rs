#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/tobixdev/rdf-fusion/main/misc/logo/logo.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/tobixdev/rdf-fusion/main/misc/logo/logo.png"
)]

//! This crate implements the SPARQL scalar and aggregate functions for [RDF Fusion](../../rdf-fusion).

pub mod aggregates;
pub mod builtin;
pub mod registry;
pub mod scalar;
