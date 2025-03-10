use crate::datatypes::rdf_blank_node::RdfBlankNode;
use crate::datatypes::rdf_named_node::RdfNamedNode;
use crate::datatypes::xsd_numeric::XsdNumeric;
use crate::datatypes::{
    RdfLanguageString, RdfSimpleLiteral, RdfTypedLiteral, RdfValue, XsdBoolean,
};
use crate::encoded::EncTermField;
use crate::DFResult;
use datafusion::arrow::array::{AsArray, UnionArray};
use datafusion::common::{internal_err, ScalarValue};

#[derive(PartialEq, Eq, Debug)]
pub enum RdfTerm<'value> {
    NamedNode(RdfNamedNode<'value>),
    BlankNode(RdfBlankNode<'value>),
    Boolean(XsdBoolean),
    Numeric(XsdNumeric),
    SimpleLiteral(RdfSimpleLiteral<'value>),
    LanguageString(RdfLanguageString<'value>),
    TypedLiteral(RdfTypedLiteral<'value>),
}

impl<'data> RdfValue<'data> for RdfTerm<'data> {
    fn from_term(term: RdfTerm<'data>) -> DFResult<Self>
    where
        Self: Sized,
    {
        Ok(term)
    }

    fn from_enc_scalar(scalar: &'data ScalarValue) -> DFResult<Self>
    where
        Self: Sized,
    {
        match scalar {
            ScalarValue::Union(Some((type_id, inner_value)), _, _) => {
                let type_id = EncTermField::try_from(*type_id)?;
                Ok(match type_id {
                    EncTermField::NamedNode => {
                        RdfTerm::NamedNode(RdfNamedNode::from_enc_scalar(scalar)?)
                    }
                    EncTermField::BlankNode => {
                        RdfTerm::BlankNode(RdfBlankNode::from_enc_scalar(scalar)?)
                    }
                    EncTermField::String => match inner_value.as_ref() {
                        ScalarValue::Struct(struct_array) => {
                            match struct_array.column(1).is_null(0) {
                                true => RdfTerm::SimpleLiteral(RdfSimpleLiteral::from_enc_scalar(
                                    scalar,
                                )?),
                                false => RdfTerm::LanguageString(
                                    RdfLanguageString::from_enc_scalar(scalar)?,
                                ),
                            }
                        }
                        _ => internal_err!("Unexpected Scalar for String")?,
                    },
                    EncTermField::Boolean => RdfTerm::Boolean(XsdBoolean::from_enc_scalar(scalar)?),
                    EncTermField::Float32
                    | EncTermField::Float64
                    | EncTermField::Decimal
                    | EncTermField::Int
                    | EncTermField::Integer => {
                        RdfTerm::Numeric(XsdNumeric::from_enc_scalar(scalar)?)
                    }
                    EncTermField::TypedLiteral => {
                        RdfTerm::TypedLiteral(RdfTypedLiteral::from_enc_scalar(scalar)?)
                    }
                    EncTermField::Null => internal_err!("Scalar was null")?,
                })
            }
            _ => internal_err!("Unexpected Scalar"),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> DFResult<Self>
    where
        Self: Sized,
    {
        let field = EncTermField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        Ok(match field {
            EncTermField::NamedNode => RdfTerm::NamedNode(
                RdfNamedNode::from_enc_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::BlankNode => RdfTerm::BlankNode(
                RdfBlankNode::from_enc_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::String => match array
                .child(field.type_id())
                .as_struct()
                .column(1)
                .is_null(offset)
            {
                true => RdfTerm::SimpleLiteral(
                    RdfSimpleLiteral::from_enc_array(array, index)
                        .expect("EncTermField and null checked"),
                ),
                false => RdfTerm::LanguageString(
                    RdfLanguageString::from_enc_array(array, index)
                        .expect("EncTermField and null checked"),
                ),
            },
            EncTermField::Boolean => RdfTerm::Boolean(
                XsdBoolean::from_enc_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::Float32
            | EncTermField::Float64
            | EncTermField::Decimal
            | EncTermField::Int
            | EncTermField::Integer => RdfTerm::Numeric(
                XsdNumeric::from_enc_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::TypedLiteral => RdfTerm::TypedLiteral(
                RdfTypedLiteral::from_enc_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::Null => internal_err!("Array value was null")?,
        })
    }
}
