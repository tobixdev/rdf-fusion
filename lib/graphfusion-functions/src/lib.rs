use datafusion::common::DataFusionError;

mod dispatcher;
mod registry;
mod scalar;

type DFResult<T> = Result<T, DataFusionError>;
