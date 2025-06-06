mod oxigraph_memory;

use datafusion::arrow::error::ArrowError;

pub use oxigraph_memory::MemoryQuadStorage;

type AResult<T> = Result<T, ArrowError>;
