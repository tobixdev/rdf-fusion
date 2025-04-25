use crate::{ScalarUnaryRdfOp, ThinResult};
use datamodel::{TermRef, ThinError};
use oxiri::Iri;
use oxrdf::NamedNode;

#[derive(Debug)]
pub struct IriRdfOp {
    base_iri: Option<Iri<String>>,
}

impl IriRdfOp {
    pub fn new(base_iri: Option<Iri<String>>) -> Self {
        Self { base_iri }
    }
}

impl ScalarUnaryRdfOp for IriRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = NamedNode;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        match value {
            TermRef::NamedNode(named_node) => Ok(named_node.into_owned()),
            TermRef::SimpleLiteral(simple_literal) => {
                let resolving_result = if let Some(base_iri) = &self.base_iri {
                    base_iri.resolve(simple_literal.value)?
                } else {
                    Iri::parse(simple_literal.value.to_owned())?
                };
                Ok(NamedNode::from(resolving_result))
            }
            _ => ThinError::expected(),
        }
    }
}
