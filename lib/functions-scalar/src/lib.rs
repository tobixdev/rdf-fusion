mod comparison;
mod conversion;
mod dates_and_times;
mod functional_forms;
mod hash;
mod numeric;
mod strings;
mod terms;

pub use comparison::*;
pub use conversion::*;
pub use dates_and_times::*;
pub use functional_forms::*;
pub use hash::*;
pub use numeric::*;
use std::fmt::Debug;
pub use strings::*;
pub use terms::*;

use rdf_fusion_model::{ThinError, ThinResult};

/// Defines the volatility of SPARQL operations.
///
/// Volatility indicates how the result of an operation might change between multiple invocations
/// with the same arguments. This volatility information is used in the engine.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum SparqlOpVolatility {
    /// The operation will always return the same result for the same arguments.
    Immutable,
    /// The operation will return the same result for the same arguments within a query,
    /// but may return different results across different queries.
    Stable,
    /// The operation may return different results on successive calls with the same arguments.
    ///
    /// Common examples are functions that compute random numbers.
    Volatile,
}

/// A super trait of all SPARQL operations.
pub trait SparqlOp: Debug + Sync + Send {}

/// A trait for SPARQL operations that take no arguments.
///
/// This trait represents functions like `NOW()` or `RAND()` that don't require
/// any input arguments but produce a result.
pub trait NullarySparqlOp: SparqlOp {
    /// The type of the result produced by this operation.
    type Result;

    /// Evaluates the operation and returns a result.
    fn evaluate(&self) -> ThinResult<Self::Result>;

    /// Indicates whether the result of the operation is volatile.
    fn volatility(&self) -> SparqlOpVolatility {
        SparqlOpVolatility::Volatile
    }
}

pub trait UnarySparqlOp: SparqlOp {
    /// The argument type.
    type Arg<'data>;
    /// The type of the result produced by this operation.
    type Result<'data>;

    /// Evaluates the operation and returns a result.
    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>>;

    /// Indicates whether the result of the operation is volatile.
    fn volatility(&self) -> SparqlOpVolatility {
        SparqlOpVolatility::Stable
    }
}

pub trait BinarySparqlOp: SparqlOp {
    /// An argument type.
    type ArgLhs<'data>;
    /// An argument type.
    type ArgRhs<'data>;
    /// The type of the result produced by this operation.
    type Result<'data>;

    /// Evaluates the operation and returns a result.
    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>>;

    /// Evaluates the operation with errors present.
    ///
    /// It is guaranteed that at least one of the arguments represents an error. Otherwise,
    /// [Self::evaluate] will be called.
    fn evaluate_error<'data>(
        &self,
        _lhs: ThinResult<Self::ArgLhs<'data>>,
        _rhs: ThinResult<Self::ArgRhs<'data>>,
    ) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }

    /// Indicates whether the result of the operation is volatile.
    fn volatility(&self) -> SparqlOpVolatility {
        SparqlOpVolatility::Stable
    }
}

pub trait TernarySparqlOp: SparqlOp {
    /// An argument type.
    type Arg0<'data>;
    /// An argument type.
    type Arg1<'data>;
    /// An argument type.
    type Arg2<'data>;
    /// The type of the result produced by this operation.
    type Result<'data>;

    /// Evaluates the operation and returns a result.
    fn evaluate<'data>(
        &self,
        arg0: Self::Arg0<'data>,
        arg1: Self::Arg1<'data>,
        arg2: Self::Arg2<'data>,
    ) -> ThinResult<Self::Result<'data>>;

    /// Evaluates the operation with errors present.
    ///
    /// It is guaranteed that at least one of the arguments represents an error. Otherwise,
    /// [Self::evaluate] will be called.
    fn evaluate_error<'data>(
        &self,
        _arg0: ThinResult<Self::Arg0<'data>>,
        _arg1: ThinResult<Self::Arg1<'data>>,
        _arg2: ThinResult<Self::Arg2<'data>>,
    ) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }

    /// Indicates whether the result of the operation is volatile.
    fn volatility(&self) -> SparqlOpVolatility {
        SparqlOpVolatility::Stable
    }
}

pub trait QuaternarySparqlOp: SparqlOp {
    /// An argument type.
    type Arg0<'data>;
    /// An argument type.
    type Arg1<'data>;
    /// An argument type.
    type Arg2<'data>;
    /// An argument type.
    type Arg3<'data>;
    /// The type of the result produced by this operation.
    type Result<'data>;

    /// Evaluates the operation and returns a result.
    fn evaluate<'data>(
        &self,
        arg0: Self::Arg0<'data>,
        arg1: Self::Arg1<'data>,
        arg2: Self::Arg2<'data>,
        arg3: Self::Arg3<'data>,
    ) -> ThinResult<Self::Result<'data>>;

    /// Evaluates the operation with errors present.
    ///
    /// It is guaranteed that at least one of the arguments represents an error. Otherwise,
    /// [Self::evaluate] will be called.
    fn evaluate_error<'data>(
        &self,
        _arg0: ThinResult<Self::Arg0<'data>>,
        _arg1: ThinResult<Self::Arg1<'data>>,
        _arg2: ThinResult<Self::Arg2<'data>>,
        _arg3: ThinResult<Self::Arg3<'data>>,
    ) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }

    /// Indicates whether the result of the operation is volatile.
    fn volatility(&self) -> SparqlOpVolatility {
        SparqlOpVolatility::Stable
    }
}

pub trait NArySparqlOp: SparqlOp {
    /// The arguments type.
    type Args<'data>;
    /// The type of the result produced by this operation.
    type Result<'data>;

    /// Evaluates the operation and returns a result.
    fn evaluate<'data>(&self, args: &[Self::Args<'data>]) -> ThinResult<Self::Result<'data>>;

    /// Evaluates the operation with errors present.
    ///
    /// It is guaranteed that at least one of the arguments represents an error. Otherwise,
    /// [Self::evaluate] will be called.
    fn evaluate_error<'data>(
        &self,
        _args: &[ThinResult<Self::Args<'data>>],
    ) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }

    /// Indicates whether the result of the operation is volatile.
    fn volatility(&self) -> SparqlOpVolatility {
        SparqlOpVolatility::Stable
    }
}
