use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Float, Numeric, TermRef, ThinError};

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
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Float;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermRef::BooleanLiteral(v) => Float::from(v),
            TermRef::SimpleLiteral(v) => v.value.parse()?,
            TermRef::NumericLiteral(numeric) => match numeric {
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
    use model::{Numeric, TermRef};

    #[test]
    fn test_enc_as_float() {
        let udf = AsFloatRdfOp::new();
        let result = udf
            .evaluate(TermRef::NumericLiteral(Numeric::Int(10.into())))
            .unwrap();
        assert_eq!(result, 10.0.into());
    }
}
