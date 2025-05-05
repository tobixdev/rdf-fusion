use crate::encoding::{EncodingArray, TermDecoder};
use crate::plain_term_encoding::{PlainTermEncoding, TermType};
use datafusion::arrow::array::{Array, AsArray, GenericStringArray, PrimitiveArray, StructArray};
use datafusion::arrow::datatypes::UInt8Type;
use model::{BlankNodeRef, LiteralRef, NamedNodeRef, TermRef, ThinError, ThinResult};

pub struct TermRefPlainTermDecoder {}

/// Extracts a sequence of term references from the given array.
impl TermDecoder<PlainTermEncoding> for TermRefPlainTermDecoder {
    fn decode_terms(array: & Self::Array) -> impl Iterator<Item = ThinResult<TermRef<'data>>> {
        let array = array.array().as_struct();

        let term_type = array.column(0).as_primitive::<UInt8Type>();

        let value = array.column(1).as_string::<i32>();
        let datatype = array.column(2).as_string::<i32>();
        let language = array.column(3).as_string::<i32>();

        (0..array.len()).map(|idx| extract_term(array, term_type, value, datatype, language, idx))
    }

    fn decode_term(array: &'data Self::Scalar) -> ThinResult<TermRef<'data>> {
        todo!()
    }
}

fn extract_term<'data>(
    array: &'data StructArray,
    term_type: &'data PrimitiveArray<UInt8Type>,
    value: &'data GenericStringArray<i32>,
    datatype: &'data GenericStringArray<i32>,
    language: &'data GenericStringArray<i32>,
    idx: usize,
) -> ThinResult<TermRef<'data>> {
    let value = array
        .is_valid(idx)
        .then(|| {
            let term_type = TermType::try_from(term_type.value(idx))
                .map_err(|_| ThinError::InternalError("Unexpected term type encoding"))?;
            Ok::<_, ThinError>(decode_term(value, datatype, language, idx, term_type))
        })
        .transpose()?;
    value.ok_or(ThinError::Expected)
}

fn decode_term<'data>(
    value: &'data GenericStringArray<i32>,
    datatype: &'data GenericStringArray<i32>,
    language: &'data GenericStringArray<i32>,
    idx: usize,
    term_type: TermType,
) -> TermRef<'data> {
    match term_type {
        TermType::NamedNode => TermRef::NamedNode(NamedNodeRef::new_unchecked(value.value(idx))),
        TermType::BlankNode => TermRef::BlankNode(BlankNodeRef::new_unchecked(value.value(idx))),
        TermType::Literal => match language.is_valid(idx) {
            false => TermRef::Literal(LiteralRef::new_typed_literal(
                value.value(idx),
                NamedNodeRef::new_unchecked(datatype.value(idx)),
            )),
            true => TermRef::Literal(LiteralRef::new_language_tagged_literal_unchecked(
                value.value(idx),
                language.value(idx),
            )),
        },
    }
}
