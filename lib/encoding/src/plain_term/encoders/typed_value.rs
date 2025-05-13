use crate::encoding::TermEncoder;
use crate::plain_term::{PlainTermArrayBuilder, PlainTermEncoding};
use crate::{DFResult, TermEncoding};
use rdf_fusion_model::{ThinResult, TypedValueRef};

#[derive(Debug)]
pub struct TypedValueRefPlainTermEncoder;

impl TermEncoder<PlainTermEncoding> for TypedValueRefPlainTermEncoder {
    type Term<'data> = TypedValueRef<'data>;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Array> {
        let iter = terms.into_iter();
        let (min, _) = iter.size_hint();
        let mut builder = PlainTermArrayBuilder::new(min);

        for term in iter {
            if let Ok(term) = term {
                let decoded = term.into_decoded();
                builder.append_term(decoded.as_ref());
            } else {
                builder.append_null();
            }
        }

        PlainTermEncoding::try_new_array(builder.finish())
    }
}
