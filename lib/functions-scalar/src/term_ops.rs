use crate::SparqlOp;
use graphfusion_model::{TermRef, ThinError, ThinResult};

pub trait UnaryRdfTermOp: SparqlOp {
    type Result<'data>;

    fn evaluate<'data>(&self, term: TermRef<'data>) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }
}

pub trait BinaryRdfTermOp: SparqlOp {
    type Result<'data>;

    fn evaluate<'data>(
        &self,
        lhs: TermRef<'data>,
        rhs: TermRef<'data>,
    ) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(
        &self,
        _lhs: ThinResult<TermRef<'data>>,
        _rhs: ThinResult<TermRef<'data>>,
    ) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }
}

pub trait TernaryRdfTermOp: SparqlOp {
    type Result<'data>;

    fn evaluate<'data>(
        &self,
        arg0: TermRef<'data>,
        arg1: TermRef<'data>,
        arg2: TermRef<'data>,
    ) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(
        &self,
        _arg0: ThinResult<TermRef<'data>>,
        _arg1: ThinResult<TermRef<'data>>,
        _arg2: ThinResult<TermRef<'data>>,
    ) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }
}

pub trait QuaternaryRdfTermOp: SparqlOp {
    type Result<'data>;

    fn evaluate<'data>(
        &self,
        arg0: TermRef<'data>,
        arg1: TermRef<'data>,
        arg2: TermRef<'data>,
        arg3: TermRef<'data>,
    ) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }
}

pub trait NAryRdfTermOp: SparqlOp {
    type Result<'data>;

    fn evaluate<'data>(&self, args: &[TermRef<'data>]) -> ThinResult<Self::Result<'data>>;

    fn evaluate_error<'data>(
        &self,
        _args: &[ThinResult<TermRef<'data>>],
    ) -> ThinResult<Self::Result<'data>> {
        ThinError::expected()
    }
}
