use crate::encoding::TermEncoder;
use crate::typed_value::{TypedValueArrayBuilder, TypedValueEncoding};
use crate::{DFResult, TermEncoding};
use datafusion::common::exec_err;
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
        TypedValueEncoding::try_new_array(value_builder.finish())
    }
}
