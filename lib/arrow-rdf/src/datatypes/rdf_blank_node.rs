use crate::datatypes::rdf_term::RdfTerm;
use crate::datatypes::RdfValue;
use crate::encoded::EncTermField;
use crate::DFResult;
use datafusion::arrow::array::{AsArray, UnionArray};
use datafusion::common::{internal_err, ScalarValue};

#[derive(PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct RdfBlankNode<'value> {
    pub id: &'value str,
}

impl<'data> RdfValue<'data> for RdfBlankNode<'data> {
    fn from_term(term: RdfTerm<'data>) -> DFResult<Self>
    where
        Self: Sized,
    {
        match term {
            RdfTerm::BlankNode(inner) => Ok(inner),
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

        if *type_id != EncTermField::BlankNode.type_id() {
            return internal_err!("Unexpected scalar type_id");
        }

        match scalar.as_ref() {
            ScalarValue::Utf8(Some(value)) => Ok(Self { id: value.as_str() }),
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> DFResult<Self> {
        let field = EncTermField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            EncTermField::BlankNode => Ok(RdfBlankNode {
                id: array
                    .child(field.type_id())
                    .as_string::<i32>()
                    .value(offset),
            }),
            _ => internal_err!("Cannot create EncBlankNode from {}.", field),
        }
    }
}
