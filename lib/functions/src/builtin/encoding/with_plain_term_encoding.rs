use crate::builtin::{BuiltinName, GraphFusionUdfFactory};
use crate::{DFResult, FunctionName};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use graphfusion_encoding::plain_term::encoders::TypedValueRefPlainTermEncoder;
use graphfusion_encoding::plain_term::PlainTermEncoding;
use graphfusion_encoding::sortable_term::SortableTermEncoding;
use graphfusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_encoding::{
    EncodingArray, EncodingName, EncodingScalar, TermDecoder, TermEncoder, TermEncoding,
};
use graphfusion_model::Term;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct WithPlainTermEncodingFactory;

impl GraphFusionUdfFactory for WithPlainTermEncodingFactory {
    fn name(&self) -> FunctionName {
        FunctionName::Builtin(BuiltinName::WithPlainTermEncoding)
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::TypedValue]
    }

    fn create_with_args(&self, _constant_args: HashMap<String, Term>) -> DFResult<Arc<ScalarUDF>> {
        let udf = ScalarUDF::new_from_impl(WithPlainTermEncoding::new(self.name()));
        Ok(Arc::new(udf))
    }
}

#[derive(Debug)]
struct WithPlainTermEncoding {
    name: String,
    signature: Signature,
}

impl WithPlainTermEncoding {
    pub fn new(name: FunctionName) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::new(
                TypeSignature::Uniform(1, vec![TypedValueEncoding::data_type()]),
                Volatility::Immutable,
            ),
        }
    }

    fn convert_array(encoding_name: EncodingName, array: ArrayRef) -> DFResult<ColumnarValue> {
        match encoding_name {
            EncodingName::PlainTerm => Ok(ColumnarValue::Array(array)),
            EncodingName::TypedValue => {
                let array = TypedValueEncoding::try_new_array(array)?;
                let input = DefaultTypedValueDecoder::decode_terms(&array);
                let result = TypedValueRefPlainTermEncoder::encode_terms(input)?;
                Ok(ColumnarValue::Array(result.into_array()))
            }
            EncodingName::Sortable => exec_err!("Cannot from sortable term."),
        }
    }

    fn convert_scalar(encoding_name: EncodingName, scalar: ScalarValue) -> DFResult<ColumnarValue> {
        match encoding_name {
            EncodingName::PlainTerm => Ok(ColumnarValue::Scalar(scalar)),
            EncodingName::TypedValue => {
                let scalar = TypedValueEncoding::try_new_scalar(scalar)?;
                let input = DefaultTypedValueDecoder::decode_term(&scalar);
                let result = TypedValueRefPlainTermEncoder::encode_term(input)?;
                Ok(ColumnarValue::Scalar(result.into_scalar_value()))
            }
            EncodingName::Sortable => exec_err!("Cannot from sortable term."),
        }
    }
}

impl ScalarUDFImpl for WithPlainTermEncoding {
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
        Ok(PlainTermEncoding::data_type())
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
