use crate::datatypes::rdf_term::RdfTerm;
use crate::datatypes::RdfValue;
use crate::encoded::EncTermField;
use crate::DFResult;
use datafusion::arrow::array::{Array, AsArray, UnionArray};
use datafusion::common::{internal_err, ScalarValue};

#[derive(PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct RdfSimpleLiteral<'value> {
    pub value: &'value str,
}

impl RdfSimpleLiteral<'_> {
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }
}

impl<'data> RdfValue<'data> for RdfSimpleLiteral<'data> {
    fn from_term(term: RdfTerm<'data>) -> DFResult<Self>
    where
        Self: Sized,
    {
        match term {
            RdfTerm::SimpleLiteral(inner) => Ok(inner),
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
                true => Ok(Self {
                    value: value.column(0).as_string::<i32>().value(0),
                }),
                false => internal_err!("Values has a language"),
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
                if !language.is_null(offset) {
                    return internal_err!("Language was not null.");
                }

                Ok(Self {
                    value: values.value(offset),
                })
            }
            _ => internal_err!("Cannot create EncSimpleLiteral from {}.", field),
        }
    }
}
