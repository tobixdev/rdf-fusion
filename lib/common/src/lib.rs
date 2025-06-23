extern crate core;

mod blank_node_mode;
pub mod error;
mod quad_storage;

pub use blank_node_mode::BlankNodeMatchingMode;
pub use quad_storage::QuadPatternEvaluator;
pub use quad_storage::QuadStorage;

pub type DFResult<T> = datafusion::error::Result<T>;
