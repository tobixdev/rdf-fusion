extern crate core;

mod paths;
mod patterns;

pub use paths::*;
pub use patterns::*;

type DFResult<T> = datafusion::error::Result<T>;
