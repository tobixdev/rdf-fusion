use crate::value_encoding::RdfTermValueEncoding;
use crate::FromArrow;
use crate::{as_term_value_array, DFResult};
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use model::{
    Decimal, Double, Float, Int, Integer, Numeric, RdfTermValueRef, ThinError, ThinResult,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncEffectiveBooleanValue {
    signature: Signature,
}

impl Default for EncEffectiveBooleanValue {
    fn default() -> Self {
        Self::new()
    }
}

impl EncEffectiveBooleanValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![RdfTermValueEncoding::datatype()]),
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

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs<'_>,
    ) -> datafusion::common::Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("Unexpected number of arguments");
        }

        match &args.args[0] {
            ColumnarValue::Array(array) => {
                let array = as_term_value_array(array.as_ref())?;
                let result = (0..args.number_rows)
                    .map(|i| {
                        RdfTermValueRef::from_array(array, i)
                            .and_then(evaluate)
                            .ok()
                    })
                    .collect::<BooleanArray>();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(scalar) => {
                let result = RdfTermValueRef::from_scalar(scalar).and_then(evaluate).ok();
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
            }
        }
    }
}

fn evaluate(value: RdfTermValueRef<'_>) -> ThinResult<bool> {
    let result = match value {
        RdfTermValueRef::BooleanLiteral(value) => value.as_bool(),
        RdfTermValueRef::NumericLiteral(value) => match value {
            Numeric::Int(value) => value != Int::from(0),
            Numeric::Integer(value) => value != Integer::from(0),
            Numeric::Float(value) => value != Float::from(0_f32),
            Numeric::Double(value) => value != Double::from(0_f64),
            Numeric::Decimal(value) => value != Decimal::from(0),
        },
        RdfTermValueRef::SimpleLiteral(value) => !value.is_empty(),
        _ => return ThinError::expected(),
    };
    Ok(result)
}
