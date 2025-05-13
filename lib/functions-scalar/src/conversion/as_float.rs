use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::TypedValueRef;
use rdf_fusion_model::{Float, Numeric, ThinError};

#[derive(Debug)]
pub struct AsFloatSparqlOp;

impl Default for AsFloatSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsFloatSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AsFloatSparqlOp {}

impl UnarySparqlOp for AsFloatSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = Float;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TypedValueRef::BooleanLiteral(v) => Float::from(v),
            TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
            TypedValueRef::NumericLiteral(numeric) => match numeric {
                Numeric::Int(v) => Float::from(v),
                Numeric::Integer(v) => Float::from(v),
                Numeric::Float(v) => v,
                Numeric::Double(v) => Float::from(v),
                Numeric::Decimal(v) => Float::from(v),
            },
            _ => return ThinError::expected(),
        };
        Ok(converted)
    }
}

#[cfg(test)]
mod tests {
    use crate::{AsFloatSparqlOp, UnarySparqlOp};
    use rdf_fusion_model::{Numeric, TypedValueRef};

    #[test]
    fn test_enc_as_float() {
        let udf = AsFloatSparqlOp::new();
        let result = udf
            .evaluate(TypedValueRef::NumericLiteral(Numeric::Int(10.into())))
            .unwrap();
        assert_eq!(result, 10.0.into());
    }
}
