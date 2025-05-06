use crate::{SparqlOp, ThinResult, UnaryTermValueOp};
use graphfusion_model::TermValueRef;
use graphfusion_model::{Float, Numeric, ThinError};

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

impl SparqlOp for AsFloatSparqlOp {
    fn name(&self) -> &str {
        "xsd:float"
    }
}

impl UnaryTermValueOp for AsFloatSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = Float;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermValueRef::BooleanLiteral(v) => Float::from(v),
            TermValueRef::SimpleLiteral(v) => v.value.parse()?,
            TermValueRef::NumericLiteral(numeric) => match numeric {
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
    use crate::{AsFloatSparqlOp, UnaryTermValueOp};
    use graphfusion_model::{Numeric, TermValueRef};

    #[test]
    fn test_enc_as_float() {
        let udf = AsFloatSparqlOp::new();
        let result = udf
            .evaluate(TermValueRef::NumericLiteral(Numeric::Int(10.into())))
            .unwrap();
        assert_eq!(result, 10.0.into());
    }
}
