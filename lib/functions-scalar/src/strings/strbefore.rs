use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::{CompatibleStringArgs, StringLiteral};

#[derive(Debug)]
pub struct StrBeforeRdfOp {}

impl StrBeforeRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for StrBeforeRdfOp {
    type ArgLhs<'data> = StringLiteral<'data>;
    type ArgRhs<'data> = StringLiteral<'data>;
    type Result<'data> = StringLiteral<'data>;

    fn evaluate<'data>(
        &self,
        arg_lhs: Self::ArgLhs<'data>,
        arg_rhs: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let args = CompatibleStringArgs::try_from(arg_lhs, arg_rhs)?;

        if let Some(position) = args.lhs.find(args.rhs) {
            Ok(StringLiteral(&args.lhs[..position], args.language))
        } else {
            Ok(StringLiteral("", args.language))
        }
    }
}
