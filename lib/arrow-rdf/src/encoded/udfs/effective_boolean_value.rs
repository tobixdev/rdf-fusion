use crate::encoded::from_encoded_term::FromEncodedTerm;
use crate::encoded::EncTerm;
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datamodel::{Decimal, Double, Float, Int, Integer, Numeric, RdfOpResult, TermRef};
use std::any::Any;

#[derive(Debug)]
pub struct EncEffectiveBooleanValue {
    signature: Signature,
}

impl EncEffectiveBooleanValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EncEffectiveBooleanValue {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_effective_boolean_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("Unexpected number of arguments");
        }

        match &args[0] {
            ColumnarValue::Array(array) => {
                let array = as_enc_term_array(array.as_ref())?;
                let results = (0..number_rows)
                    .into_iter()
                    .map(|i| TermRef::from_enc_array(array, i).and_then(evaluate));
                let result = bool::iter_into_array(results)?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(scalar) => {
                let result = TermRef::from_enc_scalar(scalar).and_then(evaluate).ok();
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
            }
        }
    }
}

fn evaluate(value: TermRef<'_>) -> RdfOpResult<bool> {
    let result = match value {
        TermRef::Boolean(value) => value.as_bool(),
        TermRef::Numeric(value) => match value {
            Numeric::Int(value) => value != Int::from(0),
            Numeric::Integer(value) => value != Integer::from(0),
            Numeric::Float(value) => value != Float::from(0f32),
            Numeric::Double(value) => value != Double::from(0f64),
            Numeric::Decimal(value) => value != Decimal::from(0),
        },
        TermRef::SimpleLiteral(value) => !value.is_empty(),
        _ => return Err(()),
    };
    Ok(result)
}
