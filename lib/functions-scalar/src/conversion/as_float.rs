use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Float, InternalTermRef, Numeric, ThinError};

#[derive(Debug)]
pub struct AsFloatRdfOp;

impl Default for AsFloatRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsFloatRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for AsFloatRdfOp {
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = Float;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            InternalTermRef::BooleanLiteral(v) => Float::from(v),
            InternalTermRef::SimpleLiteral(v) => v.value.parse()?,
            InternalTermRef::NumericLiteral(numeric) => match numeric {
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
    use crate::{AsFloatRdfOp, ScalarUnaryRdfOp};
    use model::{InternalTermRef, Numeric};

    #[test]
    fn test_enc_as_float() {
        let udf = AsFloatRdfOp::new();
        let result = udf
            .evaluate(InternalTermRef::NumericLiteral(Numeric::Int(10.into())))
            .unwrap();
        assert_eq!(result, 10.0.into());
    }
}
