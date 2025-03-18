use crate::encoded::EncTermField;
use datafusion::arrow::array::{Array, AsArray, UnionArray};
use datafusion::arrow::datatypes::{
    Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type,
};
use datafusion::common::ScalarValue;
use datamodel::{
    Boolean, Decimal, Double, Float, Int, Integer, LanguageStringRef, Numeric, RdfOpResult,
    SimpleLiteralRef, StringLiteral, TermRef, TypedLiteralRef,
};
use oxrdf::{BlankNodeRef, NamedNodeRef};

pub trait FromEncodedTerm<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized;

    fn from_enc_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self>
    where
        Self: Sized;
}

impl<'data> FromEncodedTerm<'data> for TermRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        match scalar {
            ScalarValue::Union(Some((type_id, inner_value)), _, _) => {
                let type_id = EncTermField::try_from(*type_id).map_err(|_| ())?;
                Ok(match type_id {
                    EncTermField::NamedNode => {
                        TermRef::NamedNode(NamedNodeRef::from_enc_scalar(scalar)?)
                    }
                    EncTermField::BlankNode => {
                        TermRef::BlankNode(BlankNodeRef::from_enc_scalar(scalar)?)
                    }
                    EncTermField::String => match inner_value.as_ref() {
                        ScalarValue::Struct(struct_array) => {
                            match struct_array.column(1).is_null(0) {
                                true => TermRef::SimpleLiteral(SimpleLiteralRef::from_enc_scalar(
                                    scalar,
                                )?),
                                false => TermRef::LanguageString(
                                    LanguageStringRef::from_enc_scalar(scalar)?,
                                ),
                            }
                        }
                        _ => Err(())?,
                    },
                    EncTermField::Boolean => TermRef::Boolean(Boolean::from_enc_scalar(scalar)?),
                    EncTermField::Float
                    | EncTermField::Double
                    | EncTermField::Decimal
                    | EncTermField::Int
                    | EncTermField::Integer => TermRef::Numeric(Numeric::from_enc_scalar(scalar)?),
                    EncTermField::Duration => todo!(),
                    EncTermField::TypedLiteral => {
                        TermRef::TypedLiteral(TypedLiteralRef::from_enc_scalar(scalar)?)
                    }
                    EncTermField::Null => Err(())?,
                })
            }
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        Ok(match field {
            EncTermField::NamedNode => TermRef::NamedNode(
                NamedNodeRef::from_enc_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::BlankNode => TermRef::BlankNode(
                BlankNodeRef::from_enc_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::String => match array
                .child(field.type_id())
                .as_struct()
                .column(1)
                .is_null(offset)
            {
                true => TermRef::SimpleLiteral(
                    SimpleLiteralRef::from_enc_array(array, index)
                        .expect("EncTermField and null checked"),
                ),
                false => TermRef::LanguageString(
                    LanguageStringRef::from_enc_array(array, index)
                        .expect("EncTermField and null checked"),
                ),
            },
            EncTermField::Boolean => TermRef::Boolean(
                Boolean::from_enc_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::Float
            | EncTermField::Double
            | EncTermField::Decimal
            | EncTermField::Int
            | EncTermField::Integer => TermRef::Numeric(
                Numeric::from_enc_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::Duration => todo!(),
            EncTermField::TypedLiteral => TermRef::TypedLiteral(
                TypedLiteralRef::from_enc_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::Null => Err(())?,
        })
    }
}

impl<'data> FromEncodedTerm<'data> for BlankNodeRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::BlankNode.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Utf8(Some(value)) => Ok(Self::new_unchecked(value.as_str())),
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::BlankNode => Ok(BlankNodeRef::new_unchecked(
                array
                    .child(field.type_id())
                    .as_string::<i32>()
                    .value(offset),
            )),
            _ => Err(()),
        }
    }
}

impl<'data> FromEncodedTerm<'data> for LanguageStringRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::String.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => match value.column(1).is_null(0) {
                true => Err(()),
                false => Ok(Self {
                    value: value.column(0).as_string::<i32>().value(0),
                    language: value.column(1).as_string::<i32>().value(0),
                }),
            },
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::String => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let language = array.column(1).as_string::<i32>();
                if language.is_null(offset) {
                    return Err(());
                }

                Ok(Self {
                    value: values.value(offset),
                    language: language.value(offset),
                })
            }
            _ => Err(()),
        }
    }
}
impl<'data> FromEncodedTerm<'data> for NamedNodeRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::NamedNode.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Utf8(Some(value)) => Ok(Self::new_unchecked(value.as_str())),
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::NamedNode => Ok(NamedNodeRef::new_unchecked(
                array
                    .child(field.type_id())
                    .as_string::<i32>()
                    .value(offset),
            )),
            _ => Err(()),
        }
    }
}

impl<'data> FromEncodedTerm<'data> for SimpleLiteralRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::String.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => match value.column(1).is_null(0) {
                true => Ok(Self {
                    value: value.column(0).as_string::<i32>().value(0),
                }),
                false => Err(()),
            },
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::String => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let language = array.column(1).as_string::<i32>();
                if !language.is_null(offset) {
                    return Err(());
                }

                Ok(Self {
                    value: values.value(offset),
                })
            }
            _ => Err(()),
        }
    }
}
impl<'data> FromEncodedTerm<'data> for StringLiteral<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::String.type_id() {
            return Err(());
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
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
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
            _ => Err(()),
        }
    }
}

impl<'data> FromEncodedTerm<'data> for TypedLiteralRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::TypedLiteral.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => match value.column(1).is_null(0) {
                true => Err(()),
                false => Ok(Self {
                    value: value.column(0).as_string::<i32>().value(0),
                    literal_type: value.column(1).as_string::<i32>().value(0),
                }),
            },
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
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
            _ => Err(()),
        }
    }
}

impl FromEncodedTerm<'_> for Boolean {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Boolean.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Boolean(Some(value)) => Ok(Self::from(*value)),
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::Boolean => Ok(array
                .child(field.type_id())
                .as_boolean()
                .value(offset)
                .into()),
            _ => Err(()),
        }
    }
}

impl FromEncodedTerm<'_> for Decimal {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Decimal.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Decimal128(Some(value), _, _) => Ok(Self { value: *value }),
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::Decimal => Ok(Decimal::new_from_i128_unchecked(
                array
                    .child(field.type_id())
                    .as_primitive::<Decimal128Type>()
                    .value(offset),
            )),
            _ => Err(()),
        }
    }
}

impl FromEncodedTerm<'_> for Double {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Double.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Float64(Some(value)) => Ok((*value).into()),
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::Double => Ok(array
                .child(field.type_id())
                .as_primitive::<Float64Type>()
                .value(offset)
                .into()),
            _ => Err(()),
        }
    }
}

impl FromEncodedTerm<'_> for Float {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Float.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Float32(Some(value)) => Ok((*value).into()),
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::Float => Ok(array
                .child(field.type_id())
                .as_primitive::<Float32Type>()
                .value(offset)
                .into()),
            _ => Err(()),
        }
    }
}

impl FromEncodedTerm<'_> for Int {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Int.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Int32(Some(value)) => Ok(Self::new(*value)),
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::Int => Ok(array
                .child(field.type_id())
                .as_primitive::<Int32Type>()
                .value(offset)
                .into()),
            _ => Err(()),
        }
    }
}

impl FromEncodedTerm<'_> for Integer {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Int.type_id() && *type_id != EncTermField::Integer.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Int32(Some(value)) => Ok(Self {
                value: *value as i64,
            }),
            ScalarValue::Int64(Some(value)) => Ok(Self { value: *value }),
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::Int => Ok(Self {
                value: array
                    .child(field.type_id())
                    .as_primitive::<Int32Type>()
                    .value(offset) as i64,
            }),
            EncTermField::Integer => Ok(Self {
                value: array
                    .child(field.type_id())
                    .as_primitive::<Int64Type>()
                    .value(offset),
            }),
            _ => Err(()),
        }
    }
}

impl FromEncodedTerm<'_> for Numeric {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return Err(());
        };

        let field = EncTermField::try_from(*type_id).expect("Fixed encoding");
        match field {
            EncTermField::Int => Ok(Self::Int(Int::from_enc_scalar(scalar)?)),
            EncTermField::Integer => Ok(Self::Integer(Integer::from_enc_scalar(scalar)?)),
            EncTermField::Float => Ok(Self::Float(Float::from_enc_scalar(scalar)?)),
            EncTermField::Double => Ok(Self::Double(Double::from_enc_scalar(scalar)?)),
            EncTermField::Decimal => Ok(Self::Decimal(Decimal::from_enc_scalar(scalar)?)),
            _ => Err(()),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        match field {
            EncTermField::Int => Ok(Self::Int(Int::from_enc_array(array, index)?)),
            EncTermField::Integer => Ok(Self::Integer(Integer::from_enc_array(array, index)?)),
            EncTermField::Float => Ok(Self::Float(Float::from_enc_array(array, index)?)),
            EncTermField::Double => Ok(Self::Double(Double::from_enc_array(array, index)?)),
            EncTermField::Decimal => Ok(Self::Decimal(Decimal::from_enc_array(array, index)?)),
            _ => Err(()),
        }
    }
}
