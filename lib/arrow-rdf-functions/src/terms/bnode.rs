use crate::{ScalarNullaryRdfOp, ScalarUnaryRdfOp, ThinResult};
use model::{BlankNode, BlankNodeRef, SimpleLiteralRef};

#[derive(Debug)]
pub struct BNodeRdfOp;

impl Default for BNodeRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl BNodeRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarNullaryRdfOp for BNodeRdfOp {
    type Result<'data> = BlankNode;

    fn evaluate<'data>(&self) -> ThinResult<Self::Result<'data>> {
        Ok(BlankNode::default())
    }
}

impl ScalarUnaryRdfOp for BNodeRdfOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = BlankNodeRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let bnode = BlankNodeRef::new(value.value)?;
        Ok(bnode)
    }
}
