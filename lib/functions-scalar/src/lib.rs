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
pub use strings::*;
pub use terms::*;

use datamodel::{RdfValueRef, ThinError, ThinResult};

pub trait ScalarNullaryRdfOp {
    type Result<'data>;

    fn evaluate<'data>(&self) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }
}

pub trait ScalarUnaryRdfOp {
    type Arg<'data>: RdfValueRef<'data>;
    type Result<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }
}

pub trait ScalarBinaryRdfOp {
    type ArgLhs<'data>: RdfValueRef<'data>;
    type ArgRhs<'data>: RdfValueRef<'data>;
    type Result<'data>;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }
}

pub trait ScalarTernaryRdfOp {
    type Arg0<'data>: RdfValueRef<'data>;
    type Arg1<'data>: RdfValueRef<'data>;
    type Arg2<'data>: RdfValueRef<'data>;
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
}

pub trait ScalarQuaternaryRdfOp {
    type Arg0<'data>: RdfValueRef<'data>;
    type Arg1<'data>: RdfValueRef<'data>;
    type Arg2<'data>: RdfValueRef<'data>;
    type Arg3<'data>: RdfValueRef<'data>;
    type Result<'data>;

    fn evaluate<'data>(
        &self,
        arg0: Self::Arg0<'data>,
        arg1: Self::Arg1<'data>,
        arg2: Self::Arg2<'data>,
        arg3: Self::Arg3<'data>,
    ) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }
}

pub trait ScalarNAryRdfOp {
    type Args<'data>: RdfValueRef<'data>;
    type Result<'data>;

    fn evaluate<'data>(&self, args: &[Self::Args<'data>]) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(
        &self,
        _args: &[ThinResult<Self::Args<'data>>],
    ) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }
}
