use crate::{BinaryTermValueOp, SparqlOp, ThinResult};
use graphfusion_model::{CompatibleStringArgs, StringLiteralRef};

#[derive(Debug)]
pub struct StrBeforeSparqlOp;

impl Default for StrBeforeSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrBeforeSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for StrBeforeSparqlOp {
    fn name(&self) -> &str {
        "strbefore"
    }
}

impl BinaryTermValueOp for StrBeforeSparqlOp {
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
            Ok(StringLiteralRef(&args.lhs[..position], args.language))
        } else {
            Ok(StringLiteralRef("", None))
        }
    }
}
