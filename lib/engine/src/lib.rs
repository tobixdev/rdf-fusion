extern crate core;

mod engine;
pub mod error;
mod planner;
mod quad_storage;
pub mod results;
pub mod sparql;

pub use engine::GraphFusionInstance;
pub use quad_storage::QuadStorage;

type DFResult<T> = datafusion::error::Result<T>;
