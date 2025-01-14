extern crate core;

pub mod error;
pub mod results;
pub mod sparql;
mod triple_store;

use datafusion::arrow::error::ArrowError;
pub use triple_store::TripleStore;

type DFResult<T> = datafusion::error::Result<T>;
type AResult<T> = Result<T, ArrowError>;
