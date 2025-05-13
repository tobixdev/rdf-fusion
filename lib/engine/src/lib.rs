extern crate core;

mod engine;
pub mod error;
mod planner;
mod quad_storage;
pub mod results;
pub mod sparql;

pub use engine::RdfFusionInstance;
pub use quad_storage::QuadStorage;

type DFResult<T> = datafusion::error::Result<T>;
