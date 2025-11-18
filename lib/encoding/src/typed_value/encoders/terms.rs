use crate::encoding::TermEncoder;
use crate::typed_value::{
    TypedValueArrayElementBuilder, TypedValueEncoding, TypedValueEncodingRef,
};
use crate::{EncodingArray, TermEncoding};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{TermRef, ThinResult, TypedValueRef};
use std::sync::Arc;

#[derive(Debug)]
pub struct TermRefTypedValueEncoder {
    encoding: TypedValueEncodingRef,
}

impl TermRefTypedValueEncoder {
    /// Creates a new [`TermRefTypedValueEncoder`]
    pub fn new(encoding: TypedValueEncodingRef) -> Self {
        Self { encoding }
    }
}

impl TermEncoder<TypedValueEncoding> for TermRefTypedValueEncoder {
    type Term<'data> = TermRef<'data>;

    fn encode_terms<'data>(
        &self,
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<TypedValueEncoding as TermEncoding>::Array> {
        let mut value_builder =
            TypedValueArrayElementBuilder::new(Arc::clone(&self.encoding));

        for term in terms {
            match term {
                Ok(value) => match TryInto::<TypedValueRef<'_>>::try_into(value) {
                    Ok(value) => value_builder.append_typed_value(value)?,
                    Err(_) => value_builder.append_null()?,
                },
                Err(_) => {
                    value_builder.append_null()?;
                }
            }
        }
        Ok(value_builder.finish())
    }

    fn encode_term(
        &self,
        term: ThinResult<Self::Term<'_>>,
    ) -> DFResult<<TypedValueEncoding as TermEncoding>::Scalar> {
        self.encode_terms([term])?.try_as_scalar(0)
    }
}
