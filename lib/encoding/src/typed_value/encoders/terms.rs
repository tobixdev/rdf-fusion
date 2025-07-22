use crate::encoding::TermEncoder;
use crate::typed_value::{TypedValueArrayBuilder, TypedValueEncoding, TYPED_VALUE_ENCODING};
use crate::{EncodingArray, TermEncoding};
use datafusion::common::exec_err;
use rdf_fusion_common::DFResult;
use rdf_fusion_model::{TermRef, ThinError, ThinResult, TypedValueRef};

#[derive(Debug)]
pub struct TermRefTypedValueEncoder;

impl TermEncoder<TypedValueEncoding> for TermRefTypedValueEncoder {
    type Term<'data> = TermRef<'data>;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<TypedValueEncoding as TermEncoding>::Array> {
        let mut value_builder = TypedValueArrayBuilder::default();

        for term in terms {
            match term {
                Ok(value) => match TryInto::<TypedValueRef<'_>>::try_into(value) {
                    Ok(value) => value_builder.append_typed_value(value)?,
                    Err(_) => value_builder.append_null()?,
                },
                Err(ThinError::Expected) => {
                    value_builder.append_null()?;
                }
                Err(ThinError::InternalError(err)) => {
                    return exec_err!("Error while obtaining terms: {err}")
                }
            }
        }
        TYPED_VALUE_ENCODING.try_new_array(value_builder.finish())
    }

    fn encode_term(
        term: ThinResult<Self::Term<'_>>,
    ) -> DFResult<<TypedValueEncoding as TermEncoding>::Scalar> {
        Self::encode_terms([term])?.try_as_scalar(0)
    }
}
