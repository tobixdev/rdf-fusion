use crate::encoding::TermEncoder;
use crate::plain_term::{PlainTermArrayElementBuilder, PlainTermEncoding};
use crate::{EncodingArray, TermEncoding};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{Term, ThinResult, TypedValueRef};

#[derive(Debug, Default)]
pub struct TypedValueRefPlainTermEncoder;

impl TermEncoder<PlainTermEncoding> for TypedValueRefPlainTermEncoder {
    type Term<'data> = TypedValueRef<'data>;

    fn encode_terms<'data>(
        &self,
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Array> {
        let iter = terms.into_iter();
        let (min, _) = iter.size_hint();
        let mut builder = PlainTermArrayElementBuilder::new(min);

        for term in iter {
            if let Ok(term) = term {
                let decoded: Term = term.into();
                builder.append_term(decoded.as_ref());
            } else {
                builder.append_null();
            }
        }

        Ok(builder.finish())
    }

    fn encode_term(
        &self,
        term: ThinResult<Self::Term<'_>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Scalar> {
        self.encode_terms([term])?.try_as_scalar(0)
    }
}
