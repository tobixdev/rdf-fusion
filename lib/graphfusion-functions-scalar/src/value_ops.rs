use crate::SparqlOp;
use model::RdfTermValueArg;
use model::{ThinError, ThinResult};

pub trait UnaryTermValueOp: SparqlOp {
    type Arg<'data>: RdfTermValueArg<'data>;
    type Result<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }
}

pub trait BinaryTermValueOp: SparqlOp {
    type ArgLhs<'data>: RdfTermValueArg<'data>;
    type ArgRhs<'data>: RdfTermValueArg<'data>;
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
}

pub trait TernaryRdfTermValueOp: SparqlOp {
    type Arg0<'data>: RdfTermValueArg<'data>;
    type Arg1<'data>: RdfTermValueArg<'data>;
    type Arg2<'data>: RdfTermValueArg<'data>;
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

pub trait QuaternaryRdfTermValueOp: SparqlOp {
    type Arg0<'data>: RdfTermValueArg<'data>;
    type Arg1<'data>: RdfTermValueArg<'data>;
    type Arg2<'data>: RdfTermValueArg<'data>;
    type Arg3<'data>: RdfTermValueArg<'data>;
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
}

pub trait NAryRdfTermValueOp: SparqlOp {
    type Args<'data>: RdfTermValueArg<'data>;
    type Result<'data>;

    fn evaluate<'data>(&self, args: &[Self::Args<'data>]) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(
        &self,
        _args: &[ThinResult<Self::Args<'data>>],
    ) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }
}
