use crate::encoding::TermEncoder;
use crate::plain_term::{PlainTermArrayBuilder, PlainTermEncoding};
use crate::typed_value::TypedValueEncoding;
use crate::{DFResult, TermEncoding};
use datafusion::common::exec_err;
use graphfusion_model::{TermRef, ThinError, ThinResult};

#[derive(Debug)]
pub struct DefaultPlainTermEncoder;

impl TermEncoder<PlainTermEncoding> for DefaultPlainTermEncoder {
    type Term<'data> = TermRef<'data>;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Array> {
        let mut value_builder = PlainTermArrayBuilder::default();
        for value in terms {
            match value {
                Ok(TermRef::NamedNode(value)) => value_builder.append_named_node(value),
                Ok(TermRef::BlankNode(value)) => value_builder.append_blank_node(value),
                Ok(TermRef::Literal(value)) => value_builder.append_literal(value),
                Err(ThinError::Expected) => value_builder.append_null(),
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        PlainTermEncoding::try_new_array(value_builder.finish())
    }
}
