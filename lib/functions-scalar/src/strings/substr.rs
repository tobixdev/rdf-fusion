use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::{Integer, StringLiteralRef};

#[derive(Debug)]
pub struct SubStrRdfOp {}

impl SubStrRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for SubStrRdfOp {
    type ArgLhs<'lhs> = StringLiteralRef<'lhs>;
    type ArgRhs<'lhs> = Integer;
    type Result<'data> = StringLiteralRef<'data>;

    fn evaluate<'data>(
        &self,
        arg_lhs: Self::ArgLhs<'data>,
        arg_rhs: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let index = usize::try_from(arg_rhs.try_as_i64()?).map_err(|_| ())?;
        Ok(StringLiteralRef(&arg_lhs.0[index..], arg_lhs.1))
    }
}
