mod oxigraph_memory;

use datafusion::arrow::error::ArrowError;

pub use oxigraph_memory::MemoryQuadStorage;

type DFResult<T> = datafusion::error::Result<T>;
type AResult<T> = Result<T, ArrowError>;
