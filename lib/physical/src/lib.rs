#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/tobixdev/rdf-fusion/main/misc/logo/logo.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/tobixdev/rdf-fusion/main/misc/logo/logo.png"
)]

//! Contains physical operators for [RDF Fusion](../../rdf-fusion).

extern crate core;

pub mod join;
pub mod paths;
