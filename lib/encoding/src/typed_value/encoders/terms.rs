use crate::encoding::TermEncoder;
use crate::typed_value::{TypedValueArrayElementBuilder, TypedValueEncoding};
use crate::{EncodingArray, TermEncoding};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{TermRef, ThinResult, TypedValueRef};

#[derive(Debug)]
pub struct TermRefTypedValueEncoder;

impl TermEncoder<TypedValueEncoding> for TermRefTypedValueEncoder {
    type Term<'data> = TermRef<'data>;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<TypedValueEncoding as TermEncoding>::Array> {
        let mut value_builder = TypedValueArrayElementBuilder::default();

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
        term: ThinResult<Self::Term<'_>>,
    ) -> DFResult<<TypedValueEncoding as TermEncoding>::Scalar> {
        Self::encode_terms([term])?.try_as_scalar(0)
    }
}
