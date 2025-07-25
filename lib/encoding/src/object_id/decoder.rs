use crate::encoding::TermDecoder;
use crate::object_id::ObjectIdEncoding;
use crate::{EncodingScalar, TermEncoding};
use datafusion::common::ScalarValue;
use rdf_fusion_model::{ThinError, ThinResult};

#[derive(Debug)]
pub struct DefaultObjectIdDecoder;

impl TermDecoder<ObjectIdEncoding> for DefaultObjectIdDecoder {
    type Term<'data> = u64;

    fn decode_terms(
        array: &<ObjectIdEncoding as TermEncoding>::Array,
    ) -> impl Iterator<Item = ThinResult<Self::Term<'_>>> {
        array
            .object_ids()
            .iter()
            .map(|opt| opt.ok_or(ThinError::Expected))
    }

    fn decode_term(
        scalar: &<ObjectIdEncoding as TermEncoding>::Scalar,
    ) -> ThinResult<Self::Term<'_>> {
        let ScalarValue::UInt64(scalar) = scalar.scalar_value() else {
            return ThinError::internal_error("Unexpected encoding. Should be ensured.");
        };

        match scalar {
            None => ThinError::expected(),
            Some(scalar) => Ok(*scalar),
        }
    }
}
