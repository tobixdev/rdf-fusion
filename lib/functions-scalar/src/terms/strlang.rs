use crate::{BinarySparqlOp, SparqlOp, ThinResult};
use graphfusion_model::{OwnedStringLiteral, SimpleLiteralRef};

#[derive(Debug)]
pub struct StrLangSparqlOp;

impl Default for StrLangSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrLangSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for StrLangSparqlOp {
}

impl BinarySparqlOp for StrLangSparqlOp {
    type ArgLhs<'lhs> = SimpleLiteralRef<'lhs>;
    type ArgRhs<'rhs> = SimpleLiteralRef<'rhs>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        Ok(OwnedStringLiteral::new(
            lhs.value.to_owned(),
            Some(rhs.value.to_owned().to_ascii_lowercase()),
        ))
    }
}
