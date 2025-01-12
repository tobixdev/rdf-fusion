use crate::encoded::{
    ENC_TYPE_BLANK_NODE, ENC_TYPE_NAMED_NODE, ENC_TYPE_STRING, ENC_TYPE_TYPED_LITERAL,
};
use datafusion::arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use once_cell::unsync::Lazy;
use std::clone::Clone;

pub const DEC_FIELD_NAMED_NODE: &str = "named_node";
pub const DEC_FIELD_BLANK_NODE: &str = "blank_node";
pub const DEC_FIELD_STRING: &str = "string";
pub const DEC_FIELD_TYPED_LITERAL: &str = "typed_literal";

pub const DEC_FIELDS_TERM: Lazy<UnionFields> = Lazy::new(|| {
    let fields = vec![
        Field::new(DEC_FIELD_NAMED_NODE, ENC_TYPE_NAMED_NODE.clone(), true),
        Field::new(DEC_FIELD_BLANK_NODE, ENC_TYPE_BLANK_NODE.clone(), true),
        Field::new(DEC_FIELD_STRING, ENC_TYPE_STRING.clone(), true),
        Field::new(
            DEC_FIELD_TYPED_LITERAL,
            ENC_TYPE_TYPED_LITERAL.clone(),
            true,
        ),
    ];
    UnionFields::new((0..fields.len() as i8).collect::<Vec<_>>(), fields)
});
pub const DEC_TYPE_TERM: Lazy<DataType> =
    Lazy::new(|| DataType::Union(DEC_FIELDS_TERM.clone(), UnionMode::Dense));

//
// TypeIds
//

pub const DEC_TYPE_ID_NAMED_NODE: i8 = 0;
pub const DEC_TYPE_ID_BLANK_NODE: i8 = 1;
pub const DEC_TYPE_ID_STRING: i8 = 2;
pub const DEC_TYPE_ID_TYPED_LITERAL: i8 = 3;

//
// Helper Functions
//

pub fn dec_field_idx_term(name: &str) -> i8 {
    DEC_FIELDS_TERM
        .iter()
        .filter(|(_, f)| f.name() == name)
        .next()
        .unwrap()
        .0
}

pub fn dec_idx_to_field_name(idx: i8) -> String {
    DEC_FIELDS_TERM
        .iter()
        .nth(idx as usize)
        .map(|f| f.1.name().clone())
        .unwrap()
}

pub fn dec_is_nested_rdf_term(idx: i8) -> bool {
    idx == DEC_TYPE_ID_STRING || idx == DEC_TYPE_ID_TYPED_LITERAL
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_ids() {
        assert_eq!(
            DEC_TYPE_ID_NAMED_NODE,
            dec_field_idx_term(DEC_FIELD_NAMED_NODE)
        );
        assert_eq!(
            DEC_TYPE_ID_BLANK_NODE,
            dec_field_idx_term(DEC_FIELD_BLANK_NODE)
        );
        assert_eq!(DEC_TYPE_ID_STRING, dec_field_idx_term(DEC_FIELD_STRING));
        assert_eq!(
            DEC_TYPE_ID_TYPED_LITERAL,
            dec_field_idx_term(DEC_FIELD_TYPED_LITERAL)
        );
    }
}
