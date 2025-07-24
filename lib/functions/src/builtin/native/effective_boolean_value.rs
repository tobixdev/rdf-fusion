use rdf_fusion_api::functions::BuiltinName;
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
use rdf_fusion_encoding::{TermDecoder, TermEncoding};
use rdf_fusion_model::{
    Decimal, Double, Float, Int, Integer, Numeric, ThinError, ThinResult, TypedValueRef,
};
use std::any::Any;
use std::sync::Arc;

pub fn effective_boolean_value() -> Arc<ScalarUDF> {
    let udf_impl = EffectiveBooleanValue::new();
    Arc::new(ScalarUDF::new_from_impl(udf_impl))
}

#[derive(Debug)]
struct EffectiveBooleanValue {
    name: String,
    signature: Signature,
}

impl EffectiveBooleanValue {
    /// Creates a new [EffectiveBooleanValue].
    pub fn new() -> Self {
        Self {
            name: BuiltinName::EffectiveBooleanValue.to_string(),
            signature: Signature::new(
                TypeSignature::Exact(vec![TYPED_VALUE_ENCODING.data_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EffectiveBooleanValue {
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
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        match TryInto::<[_; 1]>::try_into(args.args) {
            Ok([ColumnarValue::Array(array)]) => {
                let array = TYPED_VALUE_ENCODING.try_new_array(array)?;
                let result = DefaultTypedValueDecoder::decode_terms(&array)
                    .map(|res| res.and_then(evaluate).ok())
                    .collect::<BooleanArray>();
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            Ok([ColumnarValue::Scalar(scalar)]) => {
                let scalar = TYPED_VALUE_ENCODING.try_new_scalar(scalar)?;
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
