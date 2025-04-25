use crate::{RdfValueRef, InternalTermRef, ThinError, ThinResult};
use oxrdf::vocab::xsd;
use std::cmp::Ordering;
use std::collections::HashSet;

#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct TypedLiteral {
    pub value: String,
    pub literal_type: String,
}

impl TypedLiteral {
    pub fn as_ref(&self) -> TypedLiteralRef<'_> {
        TypedLiteralRef {
            value: &self.value,
            literal_type: &self.literal_type,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TypedLiteralRef<'value> {
    pub value: &'value str,
    pub literal_type: &'value str,
}

impl TypedLiteralRef<'_> {
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

    pub fn into_owned(self) -> TypedLiteral {
        TypedLiteral {
            value: self.value.to_owned(),
            literal_type: self.literal_type.to_owned(),
        }
    }
}

impl PartialOrd for TypedLiteralRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TypedLiteralRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(other.value)
    }
}

impl<'data> RdfValueRef<'data> for TypedLiteralRef<'data> {
    fn from_term(term: InternalTermRef<'data>) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match term {
            InternalTermRef::TypedLiteral(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}
