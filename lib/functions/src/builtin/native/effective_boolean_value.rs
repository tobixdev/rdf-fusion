use crate::builtin::{BuiltinName, GraphFusionBuiltinFactory};
use crate::DFResult;
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use graphfusion_encoding::value_encoding::decoders::DefaultTypedValueDecoder;
use graphfusion_encoding::value_encoding::TypedValueEncoding;
use graphfusion_encoding::{EncodingName, TermDecoder, TermEncoding};
use graphfusion_model::{
    Decimal, Double, Float, Int, Integer, Numeric, Term, ThinError, ThinResult, TypedValueRef,
};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
struct EffectiveBooleanValueFactory;

impl GraphFusionBuiltinFactory for EffectiveBooleanValueFactory {
    fn name(&self) -> BuiltinName {
        BuiltinName::EffectiveBooleanValue
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::TypedValue]
    }

    fn create_with_args(&self, _constant_args: HashMap<String, Term>) -> DFResult<ScalarUDF> {
        Ok(ScalarUDF::new_from_impl(EncEffectiveBooleanValue::new(
            self.name(),
        )))
    }
}

#[derive(Debug)]
pub struct EncEffectiveBooleanValue {
    name: String,
    signature: Signature,
}

impl EncEffectiveBooleanValue {
    /// TODO
    pub fn new(name: BuiltinName) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::new(
                TypeSignature::Exact(vec![TypedValueEncoding::data_type()]),
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
        &self.name
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
        match TryInto::<[_; 1]>::try_into(args.args) {
            Ok([ColumnarValue::Array(array)]) => {
                let array = TypedValueEncoding::try_new_array(array)?;
                let result = DefaultTypedValueDecoder::decode_terms(&array)
                    .map(|res| res.and_then(evaluate).ok())
                    .collect::<BooleanArray>();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            Ok([ColumnarValue::Scalar(scalar)]) => {
                let scalar = TypedValueEncoding::try_new_scalar(scalar)?;
                let result = DefaultTypedValueDecoder::decode_term(&scalar)
                    .and_then(evaluate)
                    .ok();
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
            }
            _ => exec_err!("Unexpected number of arguments"),
        }
    }
}

fn evaluate(value: TypedValueRef<'_>) -> ThinResult<bool> {
    let result = match value {
        TypedValueRef::BooleanLiteral(value) => value.as_bool(),
        TypedValueRef::NumericLiteral(value) => match value {
            Numeric::Int(value) => value != Int::from(0),
            Numeric::Integer(value) => value != Integer::from(0),
            Numeric::Float(value) => value != Float::from(0_f32),
            Numeric::Double(value) => value != Double::from(0_f64),
            Numeric::Decimal(value) => value != Decimal::from(0),
        },
        TypedValueRef::SimpleLiteral(value) => !value.is_empty(),
        _ => return ThinError::expected(),
    };
    Ok(result)
}
