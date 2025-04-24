use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::{SimpleLiteralRef, TypedLiteralRef};
use oxrdf::NamedNodeRef;

#[derive(Debug)]
pub struct StrDtRdfOp;

impl Default for StrDtRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrDtRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for StrDtRdfOp {
    type ArgLhs<'lhs> = SimpleLiteralRef<'lhs>;
    type ArgRhs<'rhs> = NamedNodeRef<'rhs>;
    type Result<'data> = TypedLiteralRef<'data>;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        Ok(TypedLiteralRef {
            value: lhs.value,
            literal_type: rhs.as_str(),
        })
    }
}
