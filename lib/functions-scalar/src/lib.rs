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

use datamodel::{RdfOpError, RdfOpResult, RdfValueRef};

pub trait ScalarNullaryRdfOp {
    type Result<'data>;

    fn evaluate<'data>(&self) -> RdfOpResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        Err(RdfOpError)
    }
}

pub trait ScalarUnaryRdfOp {
    type Arg<'data>: RdfValueRef<'data>;
    type Result<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        Err(RdfOpError)
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
    ) -> RdfOpResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        Err(RdfOpError)
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
    ) -> RdfOpResult<Self::Result<'data>>;

    fn evaluate_error<'data>(
        &self,
        _arg0: RdfOpResult<Self::Arg0<'data>>,
        _arg1: RdfOpResult<Self::Arg1<'data>>,
        _arg2: RdfOpResult<Self::Arg2<'data>>,
    ) -> RdfOpResult<Self::Result<'data>> {
        Err(RdfOpError)
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
    ) -> RdfOpResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        Err(RdfOpError)
    }
}

pub trait ScalarNAryRdfOp {
    type Args<'data>: RdfValueRef<'data>;
    type Result<'data>;

    fn evaluate<'data>(&self, args: &[Self::Args<'data>]) -> RdfOpResult<Self::Result<'data>>;

    fn evaluate_error<'data>(
        &self,
        _args: &[RdfOpResult<Self::Args<'data>>],
    ) -> RdfOpResult<Self::Result<'data>> {
        Err(RdfOpError)
    }
}
