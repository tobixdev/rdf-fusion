use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{ScalarValue, exec_datafusion_err, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::plain_term::encoders::TypedValueRefPlainTermEncoder;
use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::{
    EncodingArray, EncodingName, EncodingScalar, RdfFusionEncodings, TermDecoder,
    TermEncoder, TermEncoding,
};
use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;

pub fn with_plain_term_encoding(encodings: RdfFusionEncodings) -> Arc<ScalarUDF> {
    let udf_impl = WithPlainTermEncoding::new(encodings);
    Arc::new(ScalarUDF::new_from_impl(udf_impl))
}

/// Transforms RDF Terms into the [PlainTermEncoding](rdf_fusion_encoding::plain_term::PlainTermEncoding).
#[derive(Debug, PartialEq, Eq, Hash)]
struct WithPlainTermEncoding {
    /// The name of the UDF
    name: String,
    /// The signature
    signature: Signature,
    /// A reference to used encodings.
    encodings: RdfFusionEncodings,
}

impl WithPlainTermEncoding {
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        Self {
            name: BuiltinName::WithPlainTermEncoding.to_string(),
            signature: Signature::new(
                TypeSignature::Uniform(
                    1,
                    encodings.get_data_types(&[
                        EncodingName::PlainTerm,
                        EncodingName::TypedValue,
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
            EncodingName::PlainTerm => Ok(ColumnarValue::Array(array)),
            EncodingName::TypedValue => {
                let array = TYPED_VALUE_ENCODING.try_new_array(array)?;
                let input = DefaultTypedValueDecoder::decode_terms(&array);
                let result = TypedValueRefPlainTermEncoder::encode_terms(input)?;
                Ok(ColumnarValue::Array(result.into_array()))
            }
            EncodingName::Sortable => exec_err!("Cannot from sortable term."),
            EncodingName::ObjectId => match self.encodings.object_id_mapping() {
                None => exec_err!("Cannot from object id as no encoding is provided."),
                Some(object_id_encoding) => {
                    let array = object_id_encoding.encoding().try_new_array(array)?;
                    let decoded = object_id_encoding.decode_array(&array)?;
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
            EncodingName::PlainTerm => Ok(ColumnarValue::Scalar(scalar)),
            EncodingName::TypedValue => {
                let scalar = TYPED_VALUE_ENCODING.try_new_scalar(scalar)?;
                let input = DefaultTypedValueDecoder::decode_term(&scalar);
                let result = TypedValueRefPlainTermEncoder::encode_term(input)?;
                Ok(ColumnarValue::Scalar(result.into_scalar_value()))
            }
            EncodingName::Sortable => exec_err!("Cannot from sortable term."),
            EncodingName::ObjectId => match self.encodings.object_id_mapping() {
                None => exec_err!("Cannot from object id as no encoding is provided."),
                Some(object_id_encoding) => {
                    let scalar = object_id_encoding.encoding().try_new_scalar(scalar)?;
                    let decoded = object_id_encoding.decode_scalar(&scalar)?;
                    Ok(ColumnarValue::Scalar(decoded.into_scalar_value()))
                }
            },
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

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> datafusion::common::Result<DataType> {
        exec_err!("return_field_from_args should be called")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> DFResult<FieldRef> {
        if args.arg_fields.len() != 1 {
            return plan_err!(
                "Unexpected number of arg fields in return_field_from_args."
            );
        }

        let data_type = PLAIN_TERM_ENCODING.data_type();
        let incoming_null = args.arg_fields[0].is_nullable();
        Ok(FieldRef::new(Field::new(
            "output",
            data_type,
            incoming_null,
        )))
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
