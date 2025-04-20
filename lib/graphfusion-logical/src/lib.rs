extern crate core;

pub mod paths;
pub mod patterns;

type DFResult<T> = datafusion::error::Result<T>;
