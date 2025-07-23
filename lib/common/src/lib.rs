extern crate core;

mod blank_node_mode;
pub mod error;
pub mod quads;

pub use blank_node_mode::BlankNodeMatchingMode;

use datafusion::arrow::error::ArrowError;

pub type AResult<T> = Result<T, ArrowError>;
pub type DFResult<T> = datafusion::error::Result<T>;
