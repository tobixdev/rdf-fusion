use crate::datatypes::{RdfTerm, RdfValue};
use crate::encoded::EncTermField;
use crate::DFResult;
use datafusion::arrow::array::{AsArray, UnionArray};
use datafusion::common::{internal_err, ScalarValue};
use oxrdf::vocab::xsd;
use std::cmp::Ordering;
use std::collections::HashSet;

#[derive(PartialEq, Eq, Debug)]
pub struct RdfTypedLiteral<'value> {
    pub value: &'value str,
    pub literal_type: &'value str,
}

impl RdfTypedLiteral<'_> {
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    pub fn is_numeric(&self) -> bool {
        let numeric_types = HashSet::from([
            xsd::INTEGER.as_str(),
            xsd::DECIMAL.as_str(),
            xsd::FLOAT.as_str(),
            xsd::DOUBLE.as_str(),
            xsd::STRING.as_str(),
            xsd::BOOLEAN.as_str(),
            xsd::DATE_TIME.as_str(),
            xsd::NON_POSITIVE_INTEGER.as_str(),
            xsd::NEGATIVE_INTEGER.as_str(),
            xsd::LONG.as_str(),
            xsd::INT.as_str(),
            xsd::SHORT.as_str(),
            xsd::BYTE.as_str(),
            xsd::NON_NEGATIVE_INTEGER.as_str(),
            xsd::UNSIGNED_LONG.as_str(),
            xsd::UNSIGNED_INT.as_str(),
            xsd::UNSIGNED_SHORT.as_str(),
            xsd::UNSIGNED_BYTE.as_str(),
            xsd::POSITIVE_INTEGER.as_str(),
        ]);

        // TODO: We must check whether the literal is valid or encode all numeric types in the union

        numeric_types.contains(self.literal_type)
    }
}

impl PartialOrd for RdfTypedLiteral<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value.partial_cmp(other.value)
    }
}

impl Ord for RdfTypedLiteral<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("Ordering is total")
    }
}

impl<'data> RdfValue<'data> for RdfTypedLiteral<'data> {
    fn from_term(term: RdfTerm<'data>) -> DFResult<Self>
    where
        Self: Sized,
    {
        match term {
            RdfTerm::TypedLiteral(inner) => Ok(inner),
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

        if *type_id != EncTermField::TypedLiteral.type_id() {
            return internal_err!("Unexpected scalar type_id");
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => match value.column(1).is_null(0) {
                true => internal_err!("Values has no language"),
                false => Ok(Self {
                    value: value.column(0).as_string::<i32>().value(0),
                    literal_type: value.column(1).as_string::<i32>().value(0),
                }),
            },
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> DFResult<Self> {
        let field = EncTermField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            EncTermField::TypedLiteral => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let datatypes = array.column(1).as_string::<i32>();
                Ok(Self {
                    value: values.value(offset),
                    literal_type: datatypes.value(offset),
                })
            }
            _ => internal_err!("Cannot create EncTypedLiteral from {}.", field),
        }
    }
}
