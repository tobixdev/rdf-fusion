extern crate core;

pub mod error;
pub mod results;
pub mod sparql;
mod triple_store;

pub use triple_store::QuadStorage;

type DFResult<T> = datafusion::error::Result<T>;
