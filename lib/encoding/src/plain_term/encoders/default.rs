use crate::encoding::TermEncoder;
use crate::plain_term::{PlainTermArrayBuilder, PlainTermEncoding, PLAIN_TERM_ENCODING};
use crate::{EncodingArray, TermEncoding};
use datafusion::common::exec_err;
use rdf_fusion_common::DFResult;
use rdf_fusion_model::{TermRef, ThinError, ThinResult};

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
        PLAIN_TERM_ENCODING.try_new_array(value_builder.finish())
    }

    fn encode_term(
        term: ThinResult<Self::Term<'_>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Scalar> {
        Self::encode_terms([term])?.try_as_scalar(0)
    }
}
