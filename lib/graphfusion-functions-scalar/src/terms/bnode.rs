use crate::{NullarySparqlOp, UnaryTermValueOp, ThinResult, SparqlOp};
use model::{BlankNode, BlankNodeRef, SimpleLiteralRef};

#[derive(Debug)]
pub struct BNodeSparqlOp;

impl Default for BNodeSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl BNodeSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for BNodeSparqlOp {
    fn name(&self) -> &str {
        "bnode"
    }
}

impl NullarySparqlOp for BNodeSparqlOp {
    type Result = BlankNode;

    fn evaluate(&self) -> ThinResult<Self::Result> {
        Ok(BlankNode::default())
    }
}

impl UnaryTermValueOp for BNodeSparqlOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = BlankNodeRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let bnode = BlankNodeRef::new(value.value)?;
        Ok(bnode)
    }
}
