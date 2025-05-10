use crate::encoding::{EncodingArray, TermDecoder};
use crate::plain_term_encoding::{PlainTermEncoding, TermType};
use crate::TermEncoding;
use datafusion::arrow::array::{Array, AsArray, GenericStringArray, PrimitiveArray, StructArray};
use datafusion::arrow::datatypes::UInt8Type;
use datafusion::functions_aggregate::regr::regr_slope;
use graphfusion_model::{
    BlankNodeRef, GraphNameRef, LiteralRef, NamedNodeRef, RdfTermValue, TermRef, ThinError,
    ThinResult,
};

#[derive(Debug)]
pub struct GraphNameRefPlainTermDecoder {}

/// Extracts a sequence of term references from the given array.
impl TermDecoder<PlainTermEncoding> for GraphNameRefPlainTermDecoder {
    type Term<'data> = GraphNameRef<'data>;

    fn decode_terms(
        array: &<PlainTermEncoding as TermEncoding>::Array,
    ) -> impl Iterator<Item = ThinResult<Self::Term<'_>>> {
        let array = array.array().as_struct();

        let term_type = array.column(0).as_primitive::<UInt8Type>();

        let value = array.column(1).as_string::<i32>();

        (0..array.len()).map(|idx| extract_graph_name_ref(array, term_type, value, idx))
    }

    fn decode_term(
        array: &<PlainTermEncoding as TermEncoding>::Scalar,
    ) -> ThinResult<Self::Term<'_>> {
        todo!()
    }
}

fn extract_graph_name_ref<'data>(
    array: &'data StructArray,
    term_type: &'data PrimitiveArray<UInt8Type>,
    value: &'data GenericStringArray<i32>,
    idx: usize,
) -> ThinResult<GraphNameRef<'data>> {
    let value = array
        .is_valid(idx)
        .then(|| {
            let term_type = TermType::try_from(term_type.value(idx))
                .map_err(|_| ThinError::InternalError("Unexpected term type encoding"))?;
            decode_graph_name(value, idx, term_type)
        })
        .transpose()?;
    value.ok_or(ThinError::Expected)
}

fn decode_graph_name(
    value: &GenericStringArray<i32>,
    idx: usize,
    term_type: TermType,
) -> ThinResult<GraphNameRef<'_>> {
    let result = match term_type {
        TermType::NamedNode => {
            GraphNameRef::NamedNode(NamedNodeRef::new_unchecked(value.value(idx)))
        }
        TermType::BlankNode => {
            GraphNameRef::BlankNode(BlankNodeRef::new_unchecked(value.value(idx)))
        }
        _ => return ThinError::expected(),
    };
    Ok(result)
}
