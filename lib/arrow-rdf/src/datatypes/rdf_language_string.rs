use crate::datatypes::{RdfTerm, RdfValue};
use crate::encoded::EncTermField;
use crate::DFResult;
use datafusion::arrow::array::{Array, AsArray, UnionArray};
use datafusion::common::{internal_err, ScalarValue};
use std::cmp::Ordering;

#[derive(PartialEq, Eq, Debug)]
pub struct RdfLanguageString<'value> {
    pub value: &'value str,
    pub language: &'value str,
}

impl RdfLanguageString<'_> {
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }
}

impl PartialOrd for RdfLanguageString<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value.partial_cmp(other.value)
    }
}

impl Ord for RdfLanguageString<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("Ordering is total")
    }
}

impl<'data> RdfValue<'data> for RdfLanguageString<'data> {
    fn from_term(term: RdfTerm<'data>) -> DFResult<Self>
    where
        Self: Sized,
    {
        match term {
            RdfTerm::LanguageString(inner) => Ok(inner),
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
            ScalarValue::Struct(value) => match value.column(1).is_null(0) {
                true => internal_err!("Values has no language"),
                false => Ok(Self {
                    value: value.column(0).as_string::<i32>().value(0),
                    language: value.column(1).as_string::<i32>().value(0),
                }),
            },
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> DFResult<Self> {
        let field = EncTermField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            EncTermField::String => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let language = array.column(1).as_string::<i32>();
                if language.is_null(offset) {
                    return internal_err!("Language was null.");
                }

                Ok(Self {
                    value: values.value(offset),
                    language: language.value(offset),
                })
            }
            _ => internal_err!("Cannot create EncLanguageString from {}.", field),
        }
    }
}
