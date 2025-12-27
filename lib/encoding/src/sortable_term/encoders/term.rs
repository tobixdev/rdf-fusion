use crate::encoding::TermEncoder;
use crate::sortable_term::SortableTermEncoding;
use crate::typed_value::{TypedValueArrayElementBuilder, TypedValueEncodingRef};
use crate::{EncodingArray, TermDecoder, TermEncoding};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{TermRef, ThinError, ThinResult, TypedValueRef};
use std::sync::Arc;

#[derive(Debug)]
pub struct TermRefSortableTermEncoder {
    encoding: TypedValueEncodingRef,
}

impl TermRefSortableTermEncoder {
    /// Creates a new [`TermRefSortableTermEncoder`].
    pub fn new(encoding: TypedValueEncodingRef) -> Self {
        Self { encoding }
    }
}

impl TermEncoder<SortableTermEncoding> for TermRefSortableTermEncoder {
    type Term<'data> = TermRef<'data>;

    fn encode_terms<'data>(
        &self,
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<SortableTermEncoding as TermEncoding>::Array> {
        let mut typed_values_array =
            TypedValueArrayElementBuilder::new(Arc::clone(&self.encoding));
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
        let typed_values = typed_values_array.finish();

        // let typed_values = DefaultTypedValueDecoder::decode_terms(&typed_values);
        // TypedValueRefSortableTermEncoder.encode_terms(typed_values)
        todo!()
    }

    fn encode_term(
        &self,
        term: ThinResult<Self::Term<'_>>,
    ) -> DFResult<<SortableTermEncoding as TermEncoding>::Scalar> {
        self.encode_terms([term])?.try_as_scalar(0)
    }
}
