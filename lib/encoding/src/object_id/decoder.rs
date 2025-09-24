use crate::encoding::TermDecoder;
use crate::object_id::ObjectIdEncoding;
use crate::{EncodingScalar, TermEncoding};
use datafusion::common::ScalarValue;
use rdf_fusion_model::ObjectId;
use rdf_fusion_model::{ThinError, ThinResult};

#[derive(Debug)]
pub struct DefaultObjectIdDecoder {}

impl TermDecoder<ObjectIdEncoding> for DefaultObjectIdDecoder {
    type Term<'data> = ObjectId;

    fn decode_terms(
        array: &<ObjectIdEncoding as TermEncoding>::Array,
    ) -> impl Iterator<Item = ThinResult<Self::Term<'_>>> {
        array
            .object_ids()
            .iter()
            .map(|opt| opt.map(ObjectId::from).ok_or(ThinError::ExpectedError))
    }

    fn decode_term(
        scalar: &<ObjectIdEncoding as TermEncoding>::Scalar,
    ) -> ThinResult<Self::Term<'_>> {
        let ScalarValue::UInt32(scalar) = scalar.scalar_value() else {
            panic!("Unexpected encoding. Should be ensured by the wrapping type.");
        };

        match scalar {
            None => ThinError::expected(),
            Some(scalar) => Ok(ObjectId::from(*scalar)),
        }
    }
}
