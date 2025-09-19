use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{ScalarValue, exec_datafusion_err, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
use rdf_fusion_encoding::typed_value::encoders::TermRefTypedValueEncoder;
use rdf_fusion_encoding::{
    EncodingArray, EncodingName, EncodingScalar, RdfFusionEncodings, TermDecoder,
    TermEncoder, TermEncoding,
};
use std::any::Any;
use std::hash::{Hash, Hasher};

pub fn with_typed_value_encoding(encodings: RdfFusionEncodings) -> ScalarUDF {
    let udf_impl = WithTypedValueEncoding::new(encodings);
    ScalarUDF::new_from_impl(udf_impl)
}

/// Transforms RDF Terms into the [TypedValueEncoding](rdf_fusion_encoding::typed_value::TypedValueEncoding).
#[derive(Debug, PartialEq, Eq)]
struct WithTypedValueEncoding {
    /// The name of this function
    name: String,
    /// The signature of this function
    signature: Signature,
    /// The registered encodings
    encodings: RdfFusionEncodings,
}

impl WithTypedValueEncoding {
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        Self {
            name: BuiltinName::WithTypedValueEncoding.to_string(),
            signature: Signature::new(
                TypeSignature::Uniform(
                    1,
                    encodings.get_data_types(&[
                        EncodingName::PlainTerm,
                        EncodingName::ObjectId,
                    ]),
                ),
                Volatility::Immutable,
            ),
            encodings,
        }
    }

    fn convert_array(
        &self,
        encoding_name: EncodingName,
        array: ArrayRef,
    ) -> DFResult<ColumnarValue> {
        match encoding_name {
            EncodingName::PlainTerm => {
                let array = PLAIN_TERM_ENCODING.try_new_array(array)?;
                let input = DefaultPlainTermDecoder::decode_terms(&array);
                let result = TermRefTypedValueEncoder::encode_terms(input)?;
                Ok(ColumnarValue::Array(result.into_array()))
            }
            EncodingName::TypedValue => Ok(ColumnarValue::Array(array)),
            EncodingName::Sortable => exec_err!("Cannot from sortable term."),
            EncodingName::ObjectId => match self.encodings.object_id_mapping() {
                None => exec_err!("Cannot from object id as no encoding is provided."),
                Some(object_id_encoding) => {
                    let array = object_id_encoding.encoding().try_new_array(array)?;
                    let decoded =
                        object_id_encoding.decode_array_to_typed_value(&array)?;
                    Ok(ColumnarValue::Array(decoded.into_array()))
                }
            },
        }
    }

    fn convert_scalar(
        &self,
        encoding_name: EncodingName,
        scalar: ScalarValue,
    ) -> DFResult<ColumnarValue> {
        match encoding_name {
            EncodingName::PlainTerm => {
                let scalar = PLAIN_TERM_ENCODING.try_new_scalar(scalar)?;
                let input = DefaultPlainTermDecoder::decode_term(&scalar);
                let result = TermRefTypedValueEncoder::encode_term(input)?;
                Ok(ColumnarValue::Scalar(result.into_scalar_value()))
            }
            EncodingName::TypedValue => Ok(ColumnarValue::Scalar(scalar)),
            EncodingName::Sortable => exec_err!("Cannot from sortable term."),
            EncodingName::ObjectId => match self.encodings.object_id_mapping() {
                None => exec_err!("Cannot from object id as no encoding is provided."),
                Some(object_id_encoding) => {
                    let array = object_id_encoding.encoding().try_new_scalar(scalar)?;
                    let decoded =
                        object_id_encoding.decode_scalar_to_typed_value(&array)?;
                    Ok(ColumnarValue::Scalar(decoded.into_scalar_value()))
                }
            },
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
        Ok(TYPED_VALUE_ENCODING.data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = TryInto::<[ColumnarValue; 1]>::try_into(args.args)
            .map_err(|_| exec_datafusion_err!("Invalid number of arguments."))?;
        let encoding_name = self
            .encodings
            .try_get_encoding_name(&args[0].data_type())
            .ok_or(exec_datafusion_err!(
                "Cannot obtain encoding from argument."
            ))?;

        match args {
            [ColumnarValue::Array(array)] => self.convert_array(encoding_name, array),
            [ColumnarValue::Scalar(scalar)] => self.convert_scalar(encoding_name, scalar),
        }
    }
}

impl Hash for WithTypedValueEncoding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_any().type_id().hash(state);
    }
}
