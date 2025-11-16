use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{ScalarValue, exec_datafusion_err, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
use rdf_fusion_encoding::sortable_term::encoders::{
    TermRefSortableTermEncoder, TypedValueRefSortableTermEncoder,
};
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::{
    EncodingArray, EncodingName, EncodingScalar, RdfFusionEncodings, TermDecoder,
    TermEncoder, TermEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::hash::{Hash, Hasher};

pub fn with_sortable_term_encoding(encodings: RdfFusionEncodings) -> ScalarUDF {
    let udf_impl = WithSortableEncoding::new(encodings);
    ScalarUDF::new_from_impl(udf_impl)
}

/// Transforms RDF Terms into the [SortableTermEncoding](rdf_fusion_encoding::sortable_term::SortableTermEncoding).
#[derive(Debug, PartialEq, Eq)]
struct WithSortableEncoding {
    /// The name of this function
    name: String,
    /// The signature of this function
    signature: Signature,
    /// The registered encodings
    encodings: RdfFusionEncodings,
}

impl WithSortableEncoding {
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        Self {
            name: BuiltinName::WithSortableEncoding.to_string(),
            signature: Signature::new(
                TypeSignature::Uniform(
                    1,
                    vec![
                        encodings.plain_term().data_type().clone(),
                        encodings.typed_value().data_type().clone(),
                    ],
                ),
                Volatility::Immutable,
            ),
            encodings,
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
                let result = TermRefSortableTermEncoder::default().encode_term(input)?;
                Ok(ColumnarValue::Scalar(result.into_scalar_value()))
            }
            EncodingName::TypedValue => {
                let scalar = self.encodings.typed_value().try_new_scalar(scalar)?;
                let input = DefaultTypedValueDecoder::decode_term(&scalar);
                let result =
                    TypedValueRefSortableTermEncoder::default().encode_term(input)?;
                Ok(ColumnarValue::Scalar(result.into_scalar_value()))
            }
            EncodingName::Sortable => Ok(ColumnarValue::Scalar(scalar)),
            EncodingName::ObjectId => exec_err!("Cannot from object id."),
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
                let result = TermRefSortableTermEncoder::default().encode_terms(input)?;
                Ok(ColumnarValue::Array(result.into_array_ref()))
            }
            EncodingName::TypedValue => {
                let array = self.encodings.typed_value().try_new_array(array)?;
                let input = DefaultTypedValueDecoder::decode_terms(&array);
                let result =
                    TypedValueRefSortableTermEncoder::default().encode_terms(input)?;
                Ok(ColumnarValue::Array(result.into_array_ref()))
            }
            EncodingName::Sortable => Ok(ColumnarValue::Array(array)),
            EncodingName::ObjectId => exec_err!("Cannot from object id."),
        }
    }
}

impl ScalarUDFImpl for WithSortableEncoding {
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
        Ok(SORTABLE_TERM_ENCODING.data_type().clone())
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

impl Hash for WithSortableEncoding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_any().type_id().hash(state);
    }
}
