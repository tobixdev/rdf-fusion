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

use graphfusion_model::{ThinError, ThinResult};

/// TODO
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum SparqlOpVolatility {
    /// TODO
    Immutable,
    /// TODO
    Stable,
    /// TODO
    Volatile,
}

/// A super trait of all SPARQL operations.
pub trait SparqlOp: Debug + Sync + Send {}

/// TODO
pub trait NullarySparqlOp: SparqlOp {
    /// TODO
    type Result;

    /// TODO
    fn evaluate(&self) -> ThinResult<Self::Result>;

    /// Indicates whether the result of the operation is volatile.
    fn volatility(&self) -> SparqlOpVolatility {
        SparqlOpVolatility::Volatile
    }
}

pub trait UnarySparqlOp: SparqlOp {
    type Arg<'data>;
    type Result<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }

    /// Indicates whether the result of the operation is volatile.
    fn volatility(&self) -> SparqlOpVolatility {
        SparqlOpVolatility::Stable
    }
}

pub trait BinarySparqlOp: SparqlOp {
    type ArgLhs<'data>;
    type ArgRhs<'data>;
    type Result<'data>;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>>;

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
    type Arg0<'data>;
    type Arg1<'data>;
    type Arg2<'data>;
    type Result<'data>;

    fn evaluate<'data>(
        &self,
        arg0: Self::Arg0<'data>,
        arg1: Self::Arg1<'data>,
        arg2: Self::Arg2<'data>,
    ) -> ThinResult<Self::Result<'data>>;

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
    type Arg0<'data>;
    type Arg1<'data>;
    type Arg2<'data>;
    type Arg3<'data>;
    type Result<'data>;

    fn evaluate<'data>(
        &self,
        arg0: Self::Arg0<'data>,
        arg1: Self::Arg1<'data>,
        arg2: Self::Arg2<'data>,
        arg3: Self::Arg3<'data>,
    ) -> ThinResult<Self::Result<'data>>;

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
    type Args<'data>;
    type Result<'data>;

    fn evaluate<'data>(&self, args: &[Self::Args<'data>]) -> ThinResult<Self::Result<'data>>;

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
