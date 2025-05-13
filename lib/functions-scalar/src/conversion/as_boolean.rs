use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::TypedValueRef;
use rdf_fusion_model::{Boolean, Numeric, ThinError};

#[derive(Debug)]
pub struct AsBooleanSparqlOp;

impl Default for AsBooleanSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsBooleanSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AsBooleanSparqlOp {}

impl UnarySparqlOp for AsBooleanSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TypedValueRef::BooleanLiteral(v) => v,
            TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
            TypedValueRef::NumericLiteral(numeric) => match numeric {
                Numeric::Int(v) => Boolean::from(v),
                Numeric::Integer(v) => Boolean::from(v),
                Numeric::Float(v) => Boolean::from(v),
                Numeric::Double(v) => Boolean::from(v),
                Numeric::Decimal(v) => Boolean::from(v),
            },
            _ => return ThinError::expected(),
        };
        Ok(converted)
    }
}
