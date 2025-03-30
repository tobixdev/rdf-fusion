mod comparison;
mod conversion;
mod functional_forms;
mod numeric;
mod strings;
mod terms;

pub use comparison::*;
pub use conversion::*;
pub use functional_forms::*;
pub use numeric::*;
pub use strings::*;
pub use terms::*;

use datamodel::{RdfOpResult, RdfValueRef};

pub trait ScalarNullaryRdfOp {
    type Result<'data>;

    fn evaluate<'data>(&self) -> RdfOpResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        Err(())
    }
}

pub trait ScalarUnaryRdfOp {
    type Arg<'data>: RdfValueRef<'data>;
    type Result<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        Err(())
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
        Err(())
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

    fn evaluate_error<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        Err(())
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
        Err(())
    }
}

pub trait ScalarNAryRdfOp {
    type Args<'data>: RdfValueRef<'data>;
    type Result<'data>;

    fn evaluate<'data>(&self, args: &[Self::Args<'data>]) -> RdfOpResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        Err(())
    }
}
