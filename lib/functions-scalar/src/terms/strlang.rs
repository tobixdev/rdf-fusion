use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::{LanguageStringRef, SimpleLiteralRef};

#[derive(Debug)]
pub struct StrLangRdfOp {}

impl StrLangRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for StrLangRdfOp {
    type ArgLhs<'lhs> = SimpleLiteralRef<'lhs>;
    type ArgRhs<'rhs> = SimpleLiteralRef<'rhs>;
    type Result<'data> = LanguageStringRef<'data>;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        Ok(LanguageStringRef {
            value: lhs.value,
            language: rhs.value,
        })
    }
}
