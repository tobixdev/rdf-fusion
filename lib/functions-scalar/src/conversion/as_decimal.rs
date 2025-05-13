use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::TypedValueRef;
use rdf_fusion_model::{Decimal, Numeric, ThinError};

#[derive(Debug)]
pub struct AsDecimalSparqlOp;

impl Default for AsDecimalSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsDecimalSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AsDecimalSparqlOp {}

impl UnarySparqlOp for AsDecimalSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = Decimal;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TypedValueRef::BooleanLiteral(v) => Decimal::from(v),
            TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
            TypedValueRef::NumericLiteral(numeric) => match numeric {
                Numeric::Int(v) => Decimal::from(v),
                Numeric::Integer(v) => Decimal::from(v),
                Numeric::Float(v) => Decimal::try_from(v)?,
                Numeric::Double(v) => Decimal::try_from(v)?,
                Numeric::Decimal(v) => v,
            },
            _ => return ThinError::expected(),
        };
        Ok(converted)
    }
}
