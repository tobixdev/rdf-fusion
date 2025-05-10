extern crate core;

mod expr_builder;
pub mod paths;
pub mod patterns;

pub use expr_builder::GraphFusionExprBuilder;

type DFResult<T> = datafusion::error::Result<T>;
