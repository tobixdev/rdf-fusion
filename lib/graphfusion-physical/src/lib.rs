extern crate core;

pub mod paths;
mod planner;

pub use planner::*;

type DFResult<T> = datafusion::error::Result<T>;
