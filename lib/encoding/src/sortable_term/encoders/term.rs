use crate::encoding::TermEncoder;
use crate::sortable_term::encoders::TypedValueRefSortableTermEncoder;
use crate::sortable_term::SortableTermEncoding;
use crate::typed_value::decoders::DefaultTypedValueDecoder;
use crate::typed_value::{TypedValueArrayBuilder, TypedValueEncoding};
use crate::{DFResult, TermDecoder, TermEncoding};
use datafusion::common::exec_err;
use graphfusion_model::{TermRef, ThinError, ThinResult};

#[derive(Debug)]
pub struct TermRefSortableTermEncoder;

impl TermEncoder<SortableTermEncoding> for TermRefSortableTermEncoder {
    type Term<'data> = TermRef<'data>;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<SortableTermEncoding as TermEncoding>::Array> {
        let mut typed_values_array = TypedValueArrayBuilder::default();
        for term in terms {
            match term {
                Ok(value) => typed_values_array.append_term(value)?,
                Err(ThinError::Expected) => {
                    typed_values_array.append_null()?;
                }
                Err(ThinError::InternalError(err)) => {
                    return exec_err!("Error while obtaining terms: {err}")
                }
            }
        }
        let typed_values = TypedValueEncoding::try_new_array(typed_values_array.finish())?;

        let typed_values = DefaultTypedValueDecoder::decode_terms(&typed_values);
        TypedValueRefSortableTermEncoder::encode_terms(typed_values)
    }
}
