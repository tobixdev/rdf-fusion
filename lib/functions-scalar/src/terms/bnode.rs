use crate::{RdfOpResult, ScalarNullaryRdfOp, ScalarUnaryRdfOp};
use datamodel::{BlankNodeRef, SimpleLiteralRef};
use oxrdf::BlankNode;

#[derive(Debug)]
pub struct BNodeRdfOp {}

impl BNodeRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarNullaryRdfOp for BNodeRdfOp {
    type Result<'data> = BlankNode;

    fn evaluate<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        Ok(BlankNode::default())
    }
}

impl ScalarUnaryRdfOp for BNodeRdfOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = BlankNodeRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let bnode = BlankNodeRef::new(value.value).map_err(|_| ())?;
        Ok(bnode)
    }
}
