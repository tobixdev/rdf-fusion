extern crate core;

pub mod paths;
pub mod patterns;
mod expr_builder;

type DFResult<T> = datafusion::error::Result<T>;
