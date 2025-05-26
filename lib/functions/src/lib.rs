use datafusion::common::DataFusionError;

pub(crate) mod aggregates;
mod args;
pub mod builtin;
mod name;
pub mod registry;
pub(crate) mod scalar;

pub use args::{
    RdfFusionBuiltinArgNames, RdfFusionFunctionArg, RdfFusionFunctionArgs,
    RdfFusionFunctionArgsBuilder,
};
pub use name::FunctionName;

type DFResult<T> = Result<T, DataFusionError>;
