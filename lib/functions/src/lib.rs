use std::fmt::Display;
use datafusion::common::DataFusionError;

mod registry;
mod scalar;
mod builtin;

type DFResult<T> = Result<T, DataFusionError>;

/// TODO
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum SparqlOpArity {
    /// TODO
    Fixed(u8),
    /// TODO
    NAry,
}

impl Display for SparqlOpArity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SparqlOpArity::Fixed(n) => write!(f, "{}", n),
            SparqlOpArity::NAry => write!(f, "n-ary"),
        }
    }
}