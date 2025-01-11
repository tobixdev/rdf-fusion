use crate::encoded::{
    FIELDS_TERM, TYPE_BLANK_NODE, TYPE_FLOAT32, TYPE_FLOAT64, TYPE_ID_BOOLEAN, TYPE_INT,
    TYPE_INTEGER, TYPE_NAMED_NODE, TYPE_STRING, TYPE_TYPED_LITERAL,
};
use crate::DFResult;
use datafusion::arrow::array::{new_empty_array, ArrayRef, BooleanArray, UnionArray};
use datafusion::arrow::buffer::ScalarBuffer;
use std::sync::Arc;

pub fn cast_to_rdf_term(array: BooleanArray) -> DFResult<ArrayRef> {
    Ok(Arc::new(UnionArray::try_new(
        FIELDS_TERM.clone(),
        ScalarBuffer::from(vec![*TYPE_ID_BOOLEAN; array.len()]),
        Some(ScalarBuffer::from(
            (0..array.len() as i32).collect::<Vec<_>>(),
        )),
        vec![
            Arc::new(new_empty_array(&TYPE_NAMED_NODE)),
            Arc::new(new_empty_array(&TYPE_BLANK_NODE)),
            Arc::new(new_empty_array(&TYPE_STRING)),
            Arc::new(array),
            Arc::new(new_empty_array(&TYPE_FLOAT32)),
            Arc::new(new_empty_array(&TYPE_FLOAT64)),
            Arc::new(new_empty_array(&TYPE_INT)),
            Arc::new(new_empty_array(&TYPE_INTEGER)),
            Arc::new(new_empty_array(&TYPE_TYPED_LITERAL)),
        ],
    )?))
}
