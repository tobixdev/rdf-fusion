use crate::encoding::TermEncoder;
use crate::plain_term::{PlainTermArrayBuilder, PlainTermEncoding};
use crate::{DFResult, TermEncoding};
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{Boolean, LiteralRef, SimpleLiteralRef, ThinResult};

#[derive(Debug)]
pub struct BooleanPlainTermEncoder;

impl TermEncoder<PlainTermEncoding> for BooleanPlainTermEncoder {
    type Term<'data> = Boolean;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Array> {
        let iter = terms.into_iter();
        let (min, _) = iter.size_hint();
        let mut builder = PlainTermArrayBuilder::new(min);

        for value in iter {
            if let Ok(value) = value {
                builder.append_literal(LiteralRef::new_typed_literal(
                    &value.to_string(),
                    xsd::BOOLEAN,
                ));
            } else {
                builder.append_null();
            }
        }

        PlainTermEncoding::try_new_array(builder.finish())
    }
}

#[derive(Debug)]
pub struct SimpleLiteralRefPlainTermEncoder;

impl TermEncoder<PlainTermEncoding> for SimpleLiteralRefPlainTermEncoder {
    type Term<'data> = SimpleLiteralRef<'data>;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Array> {
        let iter = terms.into_iter();
        let (min, _) = iter.size_hint();
        let mut builder = PlainTermArrayBuilder::new(min);

        for value in iter {
            if let Ok(value) = value {
                builder.append_literal(LiteralRef::new_simple_literal(value.value));
            } else {
                builder.append_null();
            }
        }

        PlainTermEncoding::try_new_array(builder.finish())
    }
}
