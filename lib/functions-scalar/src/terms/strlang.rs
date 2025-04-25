use crate::{ScalarBinaryRdfOp, ThinResult};
use datamodel::{OwnedStringLiteral, SimpleLiteralRef};

#[derive(Debug)]
pub struct StrLangRdfOp;

impl Default for StrLangRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrLangRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for StrLangRdfOp {
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
