use crate::encoding::TermEncoder;
use crate::plain_term_encoding::PlainTermEncoding;
use crate::{DFResult, TermEncoding};
use graphfusion_model::{TermRef, ThinResult};

pub struct PlainTermDefaultEncoder;

impl TermEncoder<PlainTermEncoding> for PlainTermDefaultEncoder {
    type Term<'data> = TermRef<'data>;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Array> {
        todo!()
    }

    fn encode_term(
        term: ThinResult<Self::Term<'_>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Scalar> {
        todo!()
    }
}
