use crate::builtin::{BuiltinName, GraphFusionUdfFactory};
use crate::{DFResult, FunctionName};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use graphfusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use graphfusion_encoding::plain_term::PlainTermEncoding;
use graphfusion_encoding::typed_value::encoders::TermRefTypedValueEncoder;
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_encoding::{
    EncodingArray, EncodingName, EncodingScalar, TermDecoder, TermEncoder, TermEncoding,
};
use graphfusion_model::Term;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct WithTypedValueEncodingFactory;

impl GraphFusionUdfFactory for WithTypedValueEncodingFactory {
    fn name(&self) -> FunctionName {
        FunctionName::Builtin(BuiltinName::WithTypedValueEncoding)
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::PlainTerm]
    }

    fn create_with_args(&self, _constant_args: HashMap<String, Term>) -> DFResult<Arc<ScalarUDF>> {
        let udf = ScalarUDF::new_from_impl(WithTypedValueEncoding::new(self.name()));
        Ok(Arc::new(udf))
    }
}

#[derive(Debug)]
struct WithTypedValueEncoding {
    name: String,
    signature: Signature,
}

impl WithTypedValueEncoding {
    pub fn new(name: FunctionName) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::new(
                TypeSignature::Uniform(1, vec![PlainTermEncoding::data_type()]),
                Volatility::Immutable,
            ),
        }
    }

    fn convert_array(encoding_name: EncodingName, array: ArrayRef) -> DFResult<ColumnarValue> {
        match encoding_name {
            EncodingName::PlainTerm => {
                let array = PlainTermEncoding::try_new_array(array)?;
                let input = DefaultPlainTermDecoder::decode_terms(&array);
                let result = TermRefTypedValueEncoder::encode_terms(input)?;
                Ok(ColumnarValue::Array(result.into_array()))
            }
            EncodingName::TypedValue => Ok(ColumnarValue::Array(array)),
            EncodingName::Sortable => exec_err!("Cannot from sortable term."),
        }
    }

    fn convert_scalar(encoding_name: EncodingName, scalar: ScalarValue) -> DFResult<ColumnarValue> {
        match encoding_name {
            EncodingName::PlainTerm => {
                let scalar = PlainTermEncoding::try_new_scalar(scalar)?;
                let input = DefaultPlainTermDecoder::decode_term(&scalar);
                let result = TermRefTypedValueEncoder::encode_term(input)?;
                Ok(ColumnarValue::Scalar(result.into_scalar_value()))
            }
            EncodingName::TypedValue => Ok(ColumnarValue::Scalar(scalar)),
            EncodingName::Sortable => exec_err!("Cannot from sortable term."),
        }
    }
}

impl ScalarUDFImpl for WithTypedValueEncoding {
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
        Ok(TypedValueEncoding::data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
        let args = TryInto::<[ColumnarValue; 1]>::try_into(args.args)
            .map_err(|_| exec_datafusion_err!("Invalid number of arguments."))?;
        let encoding_name = EncodingName::try_from_data_type(&args[0].data_type()).ok_or(
            exec_datafusion_err!("Cannot obtain encoding from argument."),
        )?;

        match args {
            [ColumnarValue::Array(array)] => Self::convert_array(encoding_name, array),
            [ColumnarValue::Scalar(scalar)] => Self::convert_scalar(encoding_name, scalar),
        }
    }
}
