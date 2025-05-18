use datafusion::common::DataFusionError;

pub(crate) mod aggregates;
pub mod builtin;
mod name;
pub mod registry;
pub(crate) mod scalar;

pub use name::FunctionName;

type DFResult<T> = Result<T, DataFusionError>;
