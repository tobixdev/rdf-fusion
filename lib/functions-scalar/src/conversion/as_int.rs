use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::TypedValueRef;
use rdf_fusion_model::{Int, Numeric, ThinError};

#[derive(Debug)]
pub struct AsIntSparqlOp;

impl Default for AsIntSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsIntSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AsIntSparqlOp {}

impl UnarySparqlOp for AsIntSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = Int;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TypedValueRef::BooleanLiteral(v) => Int::from(v),
            TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
            TypedValueRef::NumericLiteral(numeric) => match numeric {
                Numeric::Int(v) => v,
                Numeric::Integer(v) => Int::try_from(v)?,
                Numeric::Float(v) => Int::try_from(v)?,
                Numeric::Double(v) => Int::try_from(v)?,
                Numeric::Decimal(v) => Int::try_from(v)?,
            },
            _ => return ThinError::expected(),
        };
        Ok(converted)
    }
}
