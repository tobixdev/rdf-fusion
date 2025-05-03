mod comparison;
mod conversion;
mod dates_and_times;
mod functional_forms;
mod hash;
mod numeric;
mod strings;
mod term_ops;
mod terms;
mod value_ops;

pub use comparison::*;
pub use conversion::*;
pub use dates_and_times::*;
pub use functional_forms::*;
pub use hash::*;
pub use numeric::*;
use std::fmt::Debug;
pub use strings::*;
pub use term_ops::*;
pub use terms::*;
pub use value_ops::*;

use model::ThinResult;

/// A super trait of all SPARQL operations.
pub trait SparqlOp: Debug {
    /// Returns the name of the SPARQL operation.
    ///
    /// SPARQL operations with the same name refer to the same function.
    fn name(&self) -> &str;
}

pub trait NullarySparqlOp: SparqlOp {
    type Result;
    fn evaluate(&self) -> ThinResult<Self::Result>;
}
