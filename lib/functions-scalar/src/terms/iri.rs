use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::TypedValueRef;
use rdf_fusion_model::{Iri, NamedNode, ThinError};

#[derive(Debug)]
pub struct IriSparqlOp {
    base_iri: Option<Iri<String>>,
}

impl IriSparqlOp {
    pub fn new(base_iri: Option<Iri<String>>) -> Self {
        Self { base_iri }
    }
}

impl SparqlOp for IriSparqlOp {}

impl UnarySparqlOp for IriSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = NamedNode;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        match value {
            TypedValueRef::NamedNode(named_node) => Ok(named_node.into_owned()),
            TypedValueRef::SimpleLiteral(simple_literal) => {
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
