use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Numeric, TermRef, StringLiteral};
use oxrdf::vocab::{rdf, xsd};

#[derive(Debug)]
pub struct DatatypeRdfOp {}

impl DatatypeRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for DatatypeRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = StringLiteral<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let datatype = match value {
            TermRef::NamedNode(_) => None,
            TermRef::BlankNode(_) => None,
            TermRef::SimpleLiteral(_) => Some(xsd::STRING.as_str()),
            TermRef::Numeric(value) => Some(match value {
                Numeric::Int(_) => xsd::INT.as_str(),
                Numeric::Integer(_) => xsd::INTEGER.as_str(),
                Numeric::Float(_) => xsd::FLOAT.as_str(),
                Numeric::Double(_) => xsd::DOUBLE.as_str(),
                Numeric::Decimal(_) => xsd::DECIMAL.as_str(),
            }),
            TermRef::Boolean(_) => Some(xsd::BOOLEAN.as_str()),
            TermRef::LanguageString(_) => Some(rdf::LANG_STRING.as_str()),
            TermRef::Duration(_) => todo!(),
            TermRef::TypedLiteral(value) => Some(value.literal_type),
        }
        .ok_or(())?;
        Ok(StringLiteral(datatype, None))
    }
}
