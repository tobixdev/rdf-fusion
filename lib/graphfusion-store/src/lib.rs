pub mod error;
mod oxigraph_memory;
mod triple_store;

use datafusion::arrow::error::ArrowError;

pub use oxigraph_memory::MemoryTripleStore;
pub use triple_store::TripleStore;

type DFResult<T> = datafusion::error::Result<T>;
type AResult<T> = Result<T, ArrowError>;
