use crate::encoding::TermEncoder;
use crate::sortable_term::SortableTermEncoding;
use crate::sortable_term::encoders::TypedValueRefSortableTermEncoder;
use crate::typed_value::decoders::DefaultTypedValueDecoder;
use crate::typed_value::{TYPED_VALUE_ENCODING, TypedValueArrayBuilder};
use crate::{EncodingArray, TermDecoder, TermEncoding};
use rdf_fusion_common::DFResult;
use rdf_fusion_model::{TermRef, ThinError, ThinResult, TypedValueRef};

#[derive(Debug)]
pub struct TermRefSortableTermEncoder;

impl TermEncoder<SortableTermEncoding> for TermRefSortableTermEncoder {
    type Term<'data> = TermRef<'data>;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<SortableTermEncoding as TermEncoding>::Array> {
        let mut typed_values_array = TypedValueArrayBuilder::default();
        for term in terms {
            let value: ThinResult<TypedValueRef<'_>> =
                term.and_then(|t| t.try_into().map_err(|_| ThinError::ExpectedError));
            match value {
                Ok(value) => typed_values_array.append_typed_value(value)?,
                Err(_) => {
                    typed_values_array.append_null()?;
                }
            }
        }
        let typed_values =
            TYPED_VALUE_ENCODING.try_new_array(typed_values_array.finish())?;

        let typed_values = DefaultTypedValueDecoder::decode_terms(&typed_values);
        TypedValueRefSortableTermEncoder::encode_terms(typed_values)
    }

    fn encode_term(
        term: ThinResult<Self::Term<'_>>,
    ) -> DFResult<<SortableTermEncoding as TermEncoding>::Scalar> {
        Self::encode_terms([term])?.try_as_scalar(0)
    }
}
