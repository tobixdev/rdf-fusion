use crate::datatypes::{RdfTerm, RdfValue};
use crate::encoded::EncTermField;
use crate::DFResult;
use datafusion::arrow::array::{Array, AsArray, UnionArray};
use datafusion::common::{internal_err, ScalarValue};
use std::cmp::Ordering;

/// https://www.w3.org/TR/sparql11-query/#func-string
#[derive(PartialEq, Eq, Debug)]
pub struct RdfStringLiteral<'value>(pub &'value str, pub Option<&'value str>);

impl RdfStringLiteral<'_> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.chars().count()
    }

    pub fn value(&self) -> &str {
        self.0
    }

    pub fn language(&self) -> Option<&str> {
        self.1
    }
}

impl PartialOrd for RdfStringLiteral<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(other.0)
    }
}

impl Ord for RdfStringLiteral<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("Ordering is total")
    }
}

impl<'data> RdfValue<'data> for RdfStringLiteral<'data> {
    fn from_term(term: RdfTerm<'data>) -> DFResult<Self>
    where
        Self: Sized,
    {
        match term {
            RdfTerm::SimpleLiteral(inner) => Ok(RdfStringLiteral(inner.value, None)),
            RdfTerm::LanguageString(inner) => {
                Ok(RdfStringLiteral(inner.value, Some(inner.language)))
            }
            _ => internal_err!("Unexpected EncRdfTerm"),
        }
    }

    fn from_enc_scalar(scalar: &'data ScalarValue) -> DFResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return internal_err!("Unexpected scalar");
        };

        if *type_id != EncTermField::String.type_id() {
            return internal_err!("Unexpected scalar type_id");
        }

        match scalar.as_ref() {
            ScalarValue::Struct(array) => {
                let value_arr = array.column(0).as_string::<i32>();
                let language_arr = array.column(1).as_string::<i32>();
                if language_arr.is_null(0) {
                    Ok(Self(value_arr.value(0), None))
                } else {
                    Ok(Self(value_arr.value(0), Some(language_arr.value(0))))
                }
            }
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> DFResult<Self> {
        let field = EncTermField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            EncTermField::String => {
                let array = array.child(field.type_id()).as_struct();
                let value_arr = array.column(0).as_string::<i32>();
                let language_arr = array.column(1).as_string::<i32>();
                if language_arr.is_null(offset) {
                    Ok(Self(value_arr.value(offset), None))
                } else {
                    Ok(Self(
                        value_arr.value(offset),
                        Some(language_arr.value(offset)),
                    ))
                }
            }
            _ => internal_err!("Cannot create EncStringLiteral from {}.", field),
        }
    }
}
