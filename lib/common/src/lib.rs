extern crate core;

pub mod error;
mod quad_storage;

pub use quad_storage::QuadPatternEvaluator;
pub use quad_storage::QuadStorage;

pub type DFResult<T> = datafusion::error::Result<T>;
