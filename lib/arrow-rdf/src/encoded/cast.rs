use crate::encoded::{EncTerm, EncTermField};
use crate::DFResult;
use datafusion::arrow::array::{new_empty_array, ArrayRef, AsArray, BooleanArray, UnionArray};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::{Float32Type, Float64Type, Int32Type, Int64Type};
use datafusion::common::{exec_err, not_impl_err, ScalarValue};
use oxrdf::vocab::xsd;
use std::sync::Arc;

pub fn cast_to_rdf_term(array: BooleanArray) -> DFResult<ArrayRef> {
    Ok(Arc::new(UnionArray::try_new(
        EncTerm::term_fields(),
        ScalarBuffer::from(vec![EncTermField::Boolean.type_id(); array.len()]),
        Some(ScalarBuffer::from(
            (0..array.len() as i32).collect::<Vec<_>>(),
        )),
        vec![
            Arc::new(new_empty_array(&EncTermField::NamedNode.data_type())),
            Arc::new(new_empty_array(&EncTermField::BlankNode.data_type())),
            Arc::new(new_empty_array(&EncTermField::String.data_type())),
            Arc::new(array),
            Arc::new(new_empty_array(&EncTermField::Float32.data_type())),
            Arc::new(new_empty_array(&EncTermField::Float64.data_type())),
            Arc::new(new_empty_array(&EncTermField::Int.data_type())),
            Arc::new(new_empty_array(&EncTermField::Integer.data_type())),
            Arc::new(new_empty_array(&EncTermField::TypedLiteral.data_type())),
        ],
    )?))
}

pub fn cast_typed_literal(
    term_field: EncTermField,
    scalar: &ScalarValue,
) -> DFResult<(String, &str)> {
    let value = cast_string(scalar);
    let datatype = match term_field {
        EncTermField::Integer => xsd::INTEGER.as_str(),
        EncTermField::TypedLiteral => {
            if let ScalarValue::Struct(scalar) = scalar {
                let datatype = scalar
                    .column_by_name("datatype")
                    .unwrap()
                    .as_string::<i32>()
                    .value(0);
                datatype
            } else {
                return exec_err!("Unexpected scalar for typed literal");
            }
        }
        _ => return not_impl_err!("cast_typed_literal datatype for {term_field}"),
    };

    Ok((value, datatype))
}

pub fn cast_string(value: &ScalarValue) -> String {
    // TODO @tobixdev with type id?
    value.to_string()
}

pub fn cast_typed_literal_array(
    rdf_terms: &UnionArray,
    term_field: EncTermField,
    offset: usize,
) -> (String, &str) {
    match term_field {
        EncTermField::Int => {
            let value = rdf_terms
                .child(term_field.type_id())
                .as_primitive::<Int32Type>()
                .value(offset);
            (value.to_string(), xsd::INT.as_str())
        }
        EncTermField::Integer => {
            let value = rdf_terms
                .child(term_field.type_id())
                .as_primitive::<Int64Type>()
                .value(offset);
            (value.to_string(), xsd::INTEGER.as_str())
        }
        EncTermField::Float32 => {
            let value = rdf_terms
                .child(term_field.type_id())
                .as_primitive::<Float32Type>()
                .value(offset);
            (value.to_string(), xsd::FLOAT.as_str())
        }
        EncTermField::Float64 => {
            let value = rdf_terms
                .child(term_field.type_id())
                .as_primitive::<Float64Type>()
                .value(offset);
            (value.to_string(), xsd::DOUBLE.as_str())
        }
        EncTermField::Boolean => {
            let value = rdf_terms
                .child(term_field.type_id())
                .as_boolean()
                .value(offset);
            (value.to_string(), xsd::BOOLEAN.as_str())
        }
        EncTermField::String => {
            let value = rdf_terms
                .child(term_field.type_id())
                .as_string::<i32>()
                .value(offset);
            (value.to_string(), xsd::STRING.as_str())
        }
        EncTermField::TypedLiteral => {
            let inner = rdf_terms.child(term_field.type_id()).as_struct();
            let value = inner
                .column_by_name("value")
                .unwrap()
                .as_string::<i32>()
                .value(offset);
            let datatype = inner
                .column_by_name("datatype")
                .unwrap()
                .as_string::<i32>()
                .value(offset);
            (value.to_string(), datatype)
        }
        _ => panic!("Expected castable to str"),
    }
}

pub fn cast_bool(scalar: &ScalarValue) -> bool {
    match scalar {
        ScalarValue::Boolean(Some(value)) => *value,
        _ => panic!("epxected castable to boolean"),
    }
}

pub fn cast_str(scalar: &ScalarValue) -> &str {
    match scalar {
        ScalarValue::Utf8(value) => value.as_ref().unwrap(),
        ScalarValue::Utf8View(value) => value.as_ref().unwrap(),
        ScalarValue::LargeUtf8(value) => value.as_ref().unwrap(),
        _ => panic!("epxected castable to i32"),
    }
}

pub fn cast_str_arr(rdf_terms: &UnionArray, term_field: EncTermField, offset: usize) -> &str {
    match term_field {
        EncTermField::NamedNode => rdf_terms
            .child(term_field.type_id())
            .as_string::<i32>()
            .value(offset),
        EncTermField::BlankNode => rdf_terms
            .child(term_field.type_id())
            .as_string::<i32>()
            .value(offset),
        _ => panic!("Expected castable to str"),
    }
}

pub fn cast_bool_arr(rdf_terms: &UnionArray, term_field: EncTermField, offset: usize) -> bool {
    match term_field {
        EncTermField::Boolean => rdf_terms
            .child(term_field.type_id())
            .as_boolean()
            .value(offset),
        _ => panic!("Expected castable to str"),
    }
}

pub fn cast_i32(scalar: &ScalarValue) -> i32 {
    match scalar {
        ScalarValue::Int8(value) => value.unwrap() as i32,
        ScalarValue::Int16(value) => value.unwrap() as i32,
        ScalarValue::Int32(value) => value.unwrap(),
        ScalarValue::UInt8(value) => value.unwrap() as i32,
        ScalarValue::UInt16(value) => value.unwrap() as i32,
        _ => panic!("epxected castable to i32"),
    }
}

pub fn cast_i32_arr(rdf_terms: &UnionArray, term_field: EncTermField, offset: usize) -> i32 {
    match term_field {
        EncTermField::Int => rdf_terms
            .child(term_field.type_id())
            .as_primitive::<Int32Type>()
            .value(offset),
        _ => panic!("Expected castable to i32"),
    }
}

pub fn cast_i64(scalar: &ScalarValue) -> i64 {
    match *scalar {
        ScalarValue::Int8(value) => value.unwrap() as i64,
        ScalarValue::Int16(value) => value.unwrap() as i64,
        ScalarValue::Int32(value) => value.unwrap() as i64,
        ScalarValue::Int64(value) => value.unwrap(),
        ScalarValue::UInt8(value) => value.unwrap() as i64,
        ScalarValue::UInt16(value) => value.unwrap() as i64,
        ScalarValue::UInt32(value) => value.unwrap() as i64,
        _ => panic!("epxected castable to i64"),
    }
}

pub fn cast_i64_arr(rdf_terms: &UnionArray, term_field: EncTermField, offset: usize) -> i64 {
    match term_field {
        EncTermField::Int => rdf_terms
            .child(term_field.type_id())
            .as_primitive::<Int32Type>()
            .value(offset) as i64,
        EncTermField::Integer => rdf_terms
            .child(term_field.type_id())
            .as_primitive::<Int64Type>()
            .value(offset),
        _ => panic!("Expected castable to i64"),
    }
}

pub fn cast_f32(scalar: &ScalarValue) -> f32 {
    match *scalar {
        ScalarValue::Int8(value) => value.unwrap() as f32,
        ScalarValue::Int16(value) => value.unwrap() as f32,
        ScalarValue::Int32(value) => value.unwrap() as f32,
        ScalarValue::UInt8(value) => value.unwrap() as f32,
        ScalarValue::UInt16(value) => value.unwrap() as f32,
        ScalarValue::UInt32(value) => value.unwrap() as f32,
        ScalarValue::Float32(value) => value.unwrap(),
        _ => panic!("epxected castable to f32"),
    }
}

pub fn cast_f32_arr(rdf_terms: &UnionArray, term_field: EncTermField, offset: usize) -> f32 {
    match term_field {
        EncTermField::Int => rdf_terms
            .child(term_field.type_id())
            .as_primitive::<Int32Type>()
            .value(offset) as f32,
        EncTermField::Float32 => rdf_terms
            .child(term_field.type_id())
            .as_primitive::<Float32Type>()
            .value(offset),
        _ => panic!("Expected castable to f32"),
    }
}

pub fn cast_f64(scalar: &ScalarValue) -> f64 {
    match *scalar {
        ScalarValue::Int8(value) => value.unwrap() as f64,
        ScalarValue::Int16(value) => value.unwrap() as f64,
        ScalarValue::Int32(value) => value.unwrap() as f64,
        ScalarValue::Int64(value) => value.unwrap() as f64,
        ScalarValue::UInt8(value) => value.unwrap() as f64,
        ScalarValue::UInt16(value) => value.unwrap() as f64,
        ScalarValue::UInt32(value) => value.unwrap() as f64,
        ScalarValue::UInt64(value) => value.unwrap() as f64,
        ScalarValue::Float32(value) => value.unwrap() as f64,
        ScalarValue::Float64(value) => value.unwrap(),
        _ => panic!("epxected castable to f64"),
    }
}

pub fn cast_f64_arr(rdf_terms: &UnionArray, term_field: EncTermField, offset: usize) -> f64 {
    match term_field {
        EncTermField::Int => rdf_terms
            .child(term_field.type_id())
            .as_primitive::<Int32Type>()
            .value(offset) as f64,
        EncTermField::Integer => rdf_terms
            .child(term_field.type_id())
            .as_primitive::<Int64Type>()
            .value(offset) as f64,
        EncTermField::Float32 => rdf_terms
            .child(term_field.type_id())
            .as_primitive::<Float32Type>()
            .value(offset) as f64,
        EncTermField::Float64 => rdf_terms
            .child(term_field.type_id())
            .as_primitive::<Float64Type>()
            .value(offset),
        _ => panic!("Expected castable to f64"),
    }
}
