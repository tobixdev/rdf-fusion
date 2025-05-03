use crate::{BinaryTermValueOp, SparqlOp, ThinResult};
use model::{CompatibleStringArgs, StringLiteralRef};

#[derive(Debug)]
pub struct StrAfterSparqlOp;

impl Default for StrAfterSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrAfterSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for StrAfterSparqlOp {
    fn name(&self) -> &str {
        "strafter"
    }
}

impl BinaryTermValueOp for StrAfterSparqlOp {
    type ArgLhs<'data> = StringLiteralRef<'data>;
    type ArgRhs<'data> = StringLiteralRef<'data>;
    type Result<'data> = StringLiteralRef<'data>;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let args = CompatibleStringArgs::try_from(lhs, rhs)?;

        if let Some(position) = args.lhs.find(args.rhs) {
            let start = position + args.rhs.len();
            Ok(StringLiteralRef(&args.lhs[start..], args.language))
        } else {
            Ok(StringLiteralRef("", None))
        }
    }
}
