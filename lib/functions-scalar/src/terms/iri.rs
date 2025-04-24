use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{RdfOpError, TermRef};
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

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        match value {
            TermRef::NamedNode(named_node) => Ok(named_node.into_owned()),
            TermRef::SimpleLiteral(simple_literal) => {
                let resolving_result = if let Some(base_iri) = &self.base_iri {
                    base_iri.resolve(simple_literal.value).map_err(|_| ())?
                } else {
                    Iri::parse(simple_literal.value.to_owned()).map_err(|_| ())?
                };
                Ok(NamedNode::from(resolving_result))
            }
            _ => Err(RdfOpError),
        }
    }
}
