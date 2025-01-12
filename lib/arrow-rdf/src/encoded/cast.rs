use crate::encoded::{
    ENC_FIELDS_TERM, ENC_TYPE_BLANK_NODE, ENC_TYPE_FLOAT32, ENC_TYPE_FLOAT64, ENC_TYPE_ID_BOOLEAN,
    ENC_TYPE_INT, ENC_TYPE_INTEGER, ENC_TYPE_NAMED_NODE, ENC_TYPE_STRING, ENC_TYPE_TYPED_LITERAL,
};
use crate::DFResult;
use datafusion::arrow::array::{new_empty_array, ArrayRef, BooleanArray, UnionArray};
use datafusion::arrow::buffer::ScalarBuffer;
use std::sync::Arc;

pub fn cast_to_rdf_term(array: BooleanArray) -> DFResult<ArrayRef> {
    Ok(Arc::new(UnionArray::try_new(
        ENC_FIELDS_TERM.clone(),
        ScalarBuffer::from(vec![ENC_TYPE_ID_BOOLEAN; array.len()]),
        Some(ScalarBuffer::from(
            (0..array.len() as i32).collect::<Vec<_>>(),
        )),
        vec![
            Arc::new(new_empty_array(&ENC_TYPE_NAMED_NODE)),
            Arc::new(new_empty_array(&ENC_TYPE_BLANK_NODE)),
            Arc::new(new_empty_array(&ENC_TYPE_STRING)),
            Arc::new(array),
            Arc::new(new_empty_array(&ENC_TYPE_FLOAT32)),
            Arc::new(new_empty_array(&ENC_TYPE_FLOAT64)),
            Arc::new(new_empty_array(&ENC_TYPE_INT)),
            Arc::new(new_empty_array(&ENC_TYPE_INTEGER)),
            Arc::new(new_empty_array(&ENC_TYPE_TYPED_LITERAL)),
        ],
    )?))
}
