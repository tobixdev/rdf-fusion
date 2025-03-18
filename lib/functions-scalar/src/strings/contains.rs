use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::{Boolean, CompatibleStringArgs, StringLiteral};

#[derive(Debug)]
pub struct ContainsRdfOp {}

impl ContainsRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for ContainsRdfOp {
    type ArgLhs<'data> = StringLiteral<'data>;
    type ArgRhs<'data> = StringLiteral<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        arg_lhs: Self::ArgLhs<'data>,
        arg_rhs: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let args = CompatibleStringArgs::try_from(arg_lhs, arg_rhs)?;
        Ok(args.lhs.contains(args.rhs).into())
    }
}
