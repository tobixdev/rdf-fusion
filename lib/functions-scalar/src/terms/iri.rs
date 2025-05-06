use crate::{SparqlOp, ThinResult, UnaryRdfTermOp, UnaryTermValueOp};
use graphfusion_model::vocab::xsd;
use graphfusion_model::TermValueRef;
use graphfusion_model::{Iri, NamedNode, TermRef, ThinError};

#[derive(Debug)]
pub struct IriSparqlOp {
    base_iri: Option<Iri<String>>,
}

impl IriSparqlOp {
    pub fn new(base_iri: Option<Iri<String>>) -> Self {
        Self { base_iri }
    }
}

impl SparqlOp for IriSparqlOp {
    fn name(&self) -> &str {
        "iri"
    }
}

impl UnaryTermValueOp for IriSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = NamedNode;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        match value {
            TermValueRef::NamedNode(named_node) => Ok(named_node.into_owned()),
            TermValueRef::SimpleLiteral(simple_literal) => {
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

impl UnaryRdfTermOp for IriSparqlOp {
    type Result<'data> = NamedNode;

    fn evaluate<'data>(&self, value: TermRef<'data>) -> ThinResult<Self::Result<'data>> {
        match value {
            TermRef::NamedNode(named_node) => Ok(named_node.into_owned()),
            TermRef::Literal(lit) if lit.datatype() == xsd::STRING => {
                let resolving_result = if let Some(base_iri) = &self.base_iri {
                    base_iri.resolve(lit.value())?
                } else {
                    Iri::parse(lit.value().to_owned())?
                };
                Ok(NamedNode::from(resolving_result))
            }
            _ => ThinError::expected(),
        }
    }
}
