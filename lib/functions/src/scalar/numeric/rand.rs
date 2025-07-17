use crate::builtin::BuiltinName;
use crate::scalar::{NullaryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rand::Rng;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingArray, EncodingName, TermEncoder, TermEncoding};
use rdf_fusion_model::{Numeric, TypedValueRef};

#[derive(Debug)]
pub struct RandSparqlOp;

impl Default for RandSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl RandSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Rand);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for RandSparqlOp {
    type Args<TEncoding: TermEncoding> = NullaryArgs;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn supported_encodings(&self) -> &[EncodingName] {
        &[EncodingName::TypedValue]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Volatile
    }

    fn return_type(&self, _target_encoding: Option<EncodingName>) -> DFResult<DataType> {
        Ok(TypedValueEncoding::data_type())
    }

    fn invoke_typed_value_encoding(
        &self,
        NullaryArgs { number_rows }: Self::Args<TypedValueEncoding>,
    ) -> DFResult<ColumnarValue> {
        let mut rng = rand::rng();
        let values = (0..number_rows).map(|_| {
            let value = rng.random::<f64>();
            Ok(TypedValueRef::NumericLiteral(Numeric::Double(value.into())))
        });
        let array = DefaultTypedValueEncoder::encode_terms(values)?;
        Ok(ColumnarValue::Array(array.into_array()))
    }
}
