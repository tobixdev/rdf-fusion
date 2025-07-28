use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{ScalarValue, exec_datafusion_err, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
use rdf_fusion_encoding::sortable_term::encoders::{
    TermRefSortableTermEncoder, TypedValueRefSortableTermEncoder,
};
use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::{
    EncodingArray, EncodingName, EncodingScalar, RdfFusionEncodings, TermDecoder,
    TermEncoder, TermEncoding,
};
use std::any::Any;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

pub fn with_sortable_term_encoding(encodings: RdfFusionEncodings) -> Arc<ScalarUDF> {
    let udf_impl = WithSortableEncoding::new(encodings);
    Arc::new(ScalarUDF::new_from_impl(udf_impl))
}

/// Transforms RDF Terms into the [SortableTermEncoding](rdf_fusion_encoding::sortable_term::SortableTermEncoding).
#[derive(Debug)]
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
                        PLAIN_TERM_ENCODING.data_type(),
                        TYPED_VALUE_ENCODING.data_type(),
                    ],
                ),
                Volatility::Immutable,
            ),
            encodings,
        }
    }

    fn convert_scalar(
        encoding_name: EncodingName,
        scalar: ScalarValue,
    ) -> DFResult<ColumnarValue> {
        match encoding_name {
            EncodingName::PlainTerm => {
                let scalar = PLAIN_TERM_ENCODING.try_new_scalar(scalar)?;
                let input = DefaultPlainTermDecoder::decode_term(&scalar);
                let result = TermRefSortableTermEncoder::encode_term(input)?;
                Ok(ColumnarValue::Scalar(result.into_scalar_value()))
            }
            EncodingName::TypedValue => {
                let scalar = TYPED_VALUE_ENCODING.try_new_scalar(scalar)?;
                let input = DefaultTypedValueDecoder::decode_term(&scalar);
                let result = TypedValueRefSortableTermEncoder::encode_term(input)?;
                Ok(ColumnarValue::Scalar(result.into_scalar_value()))
            }
            EncodingName::Sortable => Ok(ColumnarValue::Scalar(scalar)),
            EncodingName::ObjectId => exec_err!("Cannot from object id."),
        }
    }

    fn convert_array(
        encoding_name: EncodingName,
        array: ArrayRef,
    ) -> DFResult<ColumnarValue> {
        match encoding_name {
            EncodingName::PlainTerm => {
                let array = PLAIN_TERM_ENCODING.try_new_array(array)?;
                let input = DefaultPlainTermDecoder::decode_terms(&array);
                let result = TermRefSortableTermEncoder::encode_terms(input)?;
                Ok(ColumnarValue::Array(result.into_array()))
            }
            EncodingName::TypedValue => {
                let array = TYPED_VALUE_ENCODING.try_new_array(array)?;
                let input = DefaultTypedValueDecoder::decode_terms(&array);
                let result = TypedValueRefSortableTermEncoder::encode_terms(input)?;
                Ok(ColumnarValue::Array(result.into_array()))
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
        Ok(SORTABLE_TERM_ENCODING.data_type())
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
            [ColumnarValue::Array(array)] => Self::convert_array(encoding_name, array),
            [ColumnarValue::Scalar(scalar)] => {
                Self::convert_scalar(encoding_name, scalar)
            }
        }
    }

    fn hash_value(&self) -> u64 {
        let hasher = &mut DefaultHasher::new();
        self.as_any().type_id().hash(hasher);
        self.name().hash(hasher);
        hasher.finish()
    }
}
