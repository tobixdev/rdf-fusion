#![doc = include_str!("../README.md")]
#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/tobixdev/rdf-fusion/main/logo.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/tobixdev/rdf-fusion/main/logo.png"
)]

extern crate core;

mod engine;
mod planner;
pub mod results;
pub mod sparql;

pub use engine::RdfFusionContext;
