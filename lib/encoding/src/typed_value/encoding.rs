use crate::encoding::TermEncoding;
use crate::typed_value::array::TypedValueArray;
use crate::typed_value::encoders::{DefaultTypedValueEncoder, TermRefTypedValueEncoder};
use crate::typed_value::error::TypedValueEncodingCreationError;
use crate::typed_value::family::{
    NumericFamily, ResourceFamily, StringFamily, TypeFamilyRef,
};
use crate::typed_value::scalar::TypedValueScalar;
use crate::{EncodingArray, EncodingName, TermEncoder};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
use datafusion::common::ScalarValue;
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{Decimal, TermRef, ThinResult};
use std::clone::Clone;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::{Arc, LazyLock};
use thiserror::Error;

static FIELDS_STRING: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, true),
    ])
});

static FIELDS_TIMESTAMP: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new(
            "value",
            DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            false,
        ),
        Field::new("offset", DataType::Int16, true),
    ])
});

static FIELDS_TYPED_LITERAL: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("datatype", DataType::Utf8, false),
    ])
});

static FIELDS_DURATION: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("months", DataType::Int64, true),
        Field::new(
            "seconds",
            DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            true,
        ),
    ])
});

static FIELDS_TYPE: LazyLock<UnionFields> = LazyLock::new(|| {
    let fields = vec![
        Field::new(
            TypedValueEncodingField::Null.name(),
            TypedValueEncodingField::Null.data_type(),
            true,
        ),
        Field::new(
            TypedValueEncodingField::NamedNode.name(),
            TypedValueEncodingField::NamedNode.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::BlankNode.name(),
            TypedValueEncodingField::BlankNode.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::String.name(),
            TypedValueEncodingField::String.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Boolean.name(),
            TypedValueEncodingField::Boolean.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Float.name(),
            TypedValueEncodingField::Float.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Double.name(),
            TypedValueEncodingField::Double.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Decimal.name(),
            TypedValueEncodingField::Decimal.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Int.name(),
            TypedValueEncodingField::Int.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Integer.name(),
            TypedValueEncodingField::Integer.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::DateTime.name(),
            TypedValueEncodingField::DateTime.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Time.name(),
            TypedValueEncodingField::Time.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Date.name(),
            TypedValueEncodingField::Date.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Duration.name(),
            TypedValueEncodingField::Duration.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::OtherLiteral.name(),
            TypedValueEncodingField::OtherLiteral.data_type(),
            false,
        ),
    ];

    #[allow(
        clippy::cast_possible_truncation,
        reason = "We know the length of the fields"
    )]
    UnionFields::new((0..fields.len() as i8).collect::<Vec<_>>(), fields)
});

/// A cheaply clonable reference to a [`TypedValueEncoding`].
pub type TypedValueEncodingRef = Arc<TypedValueEncoding>;

/// The [`TypedValueEncoding`] stores the *value* of an RDF term as a union of possible types.
///
/// # Value Spaces
///
/// Each RDF literal type has an associated value space (e.g., `xsd:int` has the value space of
/// 32-bit integers). Transforming the transformation from the lexical space to the value space
/// might be a lossy transformation. For example, the two distinct RDF terms `"1"^^xsd::int` and
/// `"01"^^xsd::int` map to the same value. The [`TypedValueEncoding`] cannot distinguish between
/// these two terms and therefore should only be used for query parts that do not rely on this
/// distinction.
///
/// # Equality
///
/// Two typed value encodings are considered to be equal if they use the same data type for
/// encoding the values. As the type family name is encoded in the union fields of the encoding,
/// the equality of the type families (which is based on their name) is also considered.
#[derive(Debug, Clone)]
pub struct TypedValueEncoding {
    /// The data type of this encoding instance.
    data_type: DataType,
    /// The registered type families.
    type_families: Vec<TypeFamilyRef>,
}

impl TypedValueEncoding {
    /// Creates a new [`TypedValueEncoding`].
    ///
    /// # Errors
    ///
    /// Returns an error if ...
    /// - more than one type families with the same id are provided.
    /// - the set of claimed types overlaps between two families.
    pub fn try_new(
        type_families: Vec<TypeFamilyRef>,
    ) -> Result<Self, TypedValueEncodingCreationError> {
        let mut seen = HashSet::new();
        for type_family in &type_families {
            let already_there = seen.insert(type_family.id());
            assert!(
                already_there,
                "Duplicate type family ID: {}",
                type_family.id()
            );

            // TODO validate name != null
        }

        // TODO check claims

        Ok(Self {
            data_type: build_data_type(&type_families),
            type_families,
        })
    }

    /// Tries to find a registered [`TypeFamilyRef`] with the given name.
    pub fn find_type_family(&self, id: &str) -> Option<&TypeFamilyRef> {
        self.type_families.iter().find(|f| f.id() == id)
    }

    /// Creates a new [`DefaultTypedValueEncoder`].
    pub fn default_encoder(self: &Arc<Self>) -> DefaultTypedValueEncoder {
        DefaultTypedValueEncoder::new(Arc::clone(self))
    }

    /// Creates a new [`TermRefTypedValueEncoder`].
    pub fn term_encoder(self: &Arc<Self>) -> TermRefTypedValueEncoder {
        TermRefTypedValueEncoder::new(Arc::clone(self))
    }
}

/// Creates a [`DataType::Union`] for the specifies type families.
fn build_data_type(families: &[TypeFamilyRef]) -> DataType {
    let mut fields = Vec::new();
    fields.push(Field::new("null", DataType::Null, false));

    for family in families {
        fields.push(Field::new(family.id(), family.data_type().clone(), false));
    }

    DataType::Union(
        UnionFields::new((0..fields.len()).collect::<Vec<_>>(), fields),
        UnionMode::Dense,
    )
}

impl Default for TypedValueEncoding {
    fn default() -> Self {
        let families = vec![
            Arc::new(ResourceFamily::new()),
            Arc::new(StringFamily::new()),
            Arc::new(NumericFamily::new()),
        ];
        Self::try_new(families).expect("No overlap, No duplicate")
    }
}

impl TypedValueEncoding {
    pub fn fields() -> UnionFields {
        FIELDS_TYPE.clone()
    }

    pub fn string_fields() -> Fields {
        FIELDS_STRING.clone()
    }

    pub fn string_type() -> DataType {
        DataType::Struct(Self::string_fields())
    }

    pub fn timestamp_fields() -> Fields {
        FIELDS_TIMESTAMP.clone()
    }

    pub fn duration_fields() -> Fields {
        FIELDS_DURATION.clone()
    }

    pub fn typed_literal_fields() -> Fields {
        FIELDS_TYPED_LITERAL.clone()
    }

    /// Encodes the `term` as a [TypedValueScalar].
    pub fn encode_term(
        &self,
        term: ThinResult<TermRef<'_>>,
    ) -> DFResult<TypedValueScalar> {
        let arc = Arc::new(self.clone());
        TermRefTypedValueEncoder::new(arc)
            .encode_terms([term])?
            .try_as_scalar(0)
    }
}

impl TermEncoding for TypedValueEncoding {
    type Array = TypedValueArray;
    type Scalar = TypedValueScalar;

    fn name(&self) -> EncodingName {
        EncodingName::TypedValue
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn try_new_array(self: &Arc<Self>, array: ArrayRef) -> DFResult<Self::Array> {
        TypedValueArray::try_new(Arc::clone(self), array)
    }

    fn try_new_scalar(self: &Arc<Self>, scalar: ScalarValue) -> DFResult<Self::Scalar> {
        TypedValueScalar::try_new(Arc::clone(self), scalar)
    }
}

impl PartialEq for TypedValueEncoding {
    fn eq(&self, other: &Self) -> bool {
        self.data_type.eq(&other.data_type)
    }
}

impl Eq for TypedValueEncoding {}

impl Hash for TypedValueEncoding {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.data_type.hash(state);
    }
}

#[repr(i8)]
#[derive(Ord, PartialOrd, PartialEq, Eq, Debug, Clone, Copy)]
pub enum TypedValueEncodingField {
    /// Represents an unbound value or an error.
    ///
    /// This has to be the first encoded field as OUTER joins will use it to initialize default
    /// values for non-matching rows.
    Null,
    NamedNode,
    BlankNode,
    String,
    Boolean,
    Float,
    Double,
    Decimal,
    Int,
    Integer,
    DateTime,
    Time,
    Date,
    Duration,
    OtherLiteral,
}

impl TypedValueEncodingField {
    pub fn type_id(self) -> i8 {
        self.into()
    }

    pub fn name(self) -> &'static str {
        match self {
            TypedValueEncodingField::Null => "null",
            TypedValueEncodingField::NamedNode => "named_node",
            TypedValueEncodingField::BlankNode => "blank_node",
            TypedValueEncodingField::String => "string",
            TypedValueEncodingField::Boolean => "boolean",
            TypedValueEncodingField::Float => "float",
            TypedValueEncodingField::Double => "double",
            TypedValueEncodingField::Decimal => "decimal",
            TypedValueEncodingField::Int => "int",
            TypedValueEncodingField::Integer => "integer",
            TypedValueEncodingField::DateTime => "date_time",
            TypedValueEncodingField::Time => "time",
            TypedValueEncodingField::Date => "date",
            TypedValueEncodingField::Duration => "duration",
            TypedValueEncodingField::OtherLiteral => "other_literal",
        }
    }

    pub fn data_type(self) -> DataType {
        match self {
            TypedValueEncodingField::Null => DataType::Null,
            TypedValueEncodingField::NamedNode | TypedValueEncodingField::BlankNode => {
                DataType::Utf8
            }
            TypedValueEncodingField::String => DataType::Struct(FIELDS_STRING.clone()),
            TypedValueEncodingField::Boolean => DataType::Boolean,
            TypedValueEncodingField::Float => DataType::Float32,
            TypedValueEncodingField::Double => DataType::Float64,
            TypedValueEncodingField::Decimal => {
                DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE)
            }
            TypedValueEncodingField::Int => DataType::Int32,
            TypedValueEncodingField::Integer => DataType::Int64,
            TypedValueEncodingField::DateTime
            | TypedValueEncodingField::Time
            | TypedValueEncodingField::Date => DataType::Struct(FIELDS_TIMESTAMP.clone()),
            TypedValueEncodingField::Duration => {
                DataType::Struct(FIELDS_DURATION.clone())
            }
            TypedValueEncodingField::OtherLiteral => {
                DataType::Struct(FIELDS_TYPED_LITERAL.clone())
            }
        }
    }

    pub fn is_literal(self) -> bool {
        matches!(
            self,
            TypedValueEncodingField::NamedNode | TypedValueEncodingField::BlankNode
        )
    }
}

impl Display for TypedValueEncodingField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug, Clone, Copy, Default, Error, PartialEq, Eq, Hash)]
pub struct UnknownTypedValueEncodingFieldError;

impl Display for UnknownTypedValueEncodingFieldError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unexpected type_id for encoded RDF Term")
    }
}

impl TryFrom<i8> for TypedValueEncodingField {
    type Error = UnknownTypedValueEncodingFieldError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => TypedValueEncodingField::Null,
            1 => TypedValueEncodingField::NamedNode,
            2 => TypedValueEncodingField::BlankNode,
            3 => TypedValueEncodingField::String,
            4 => TypedValueEncodingField::Boolean,
            5 => TypedValueEncodingField::Float,
            6 => TypedValueEncodingField::Double,
            7 => TypedValueEncodingField::Decimal,
            8 => TypedValueEncodingField::Int,
            9 => TypedValueEncodingField::Integer,
            10 => TypedValueEncodingField::DateTime,
            11 => TypedValueEncodingField::Time,
            12 => TypedValueEncodingField::Date,
            13 => TypedValueEncodingField::Duration,
            14 => TypedValueEncodingField::OtherLiteral,
            _ => return Err(UnknownTypedValueEncodingFieldError),
        })
    }
}

impl TryFrom<u8> for TypedValueEncodingField {
    type Error = UnknownTypedValueEncodingFieldError;

    #[allow(
        clippy::cast_possible_wrap,
        reason = "Self::try_from will catch any overflow as EncTermField does not have that many variants"
    )]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::try_from(value as i8)
    }
}

impl From<TypedValueEncodingField> for i8 {
    fn from(value: TypedValueEncodingField) -> Self {
        match value {
            TypedValueEncodingField::Null => 0,
            TypedValueEncodingField::NamedNode => 1,
            TypedValueEncodingField::BlankNode => 2,
            TypedValueEncodingField::String => 3,
            TypedValueEncodingField::Boolean => 4,
            TypedValueEncodingField::Float => 5,
            TypedValueEncodingField::Double => 6,
            TypedValueEncodingField::Decimal => 7,
            TypedValueEncodingField::Int => 8,
            TypedValueEncodingField::Integer => 9,
            TypedValueEncodingField::DateTime => 10,
            TypedValueEncodingField::Time => 11,
            TypedValueEncodingField::Date => 12,
            TypedValueEncodingField::Duration => 13,
            TypedValueEncodingField::OtherLiteral => 14,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_ids() {
        test_type_id(TypedValueEncodingField::NamedNode);
        test_type_id(TypedValueEncodingField::BlankNode);
        test_type_id(TypedValueEncodingField::String);
        test_type_id(TypedValueEncodingField::Boolean);
        test_type_id(TypedValueEncodingField::Float);
        test_type_id(TypedValueEncodingField::Double);
        test_type_id(TypedValueEncodingField::Decimal);
        test_type_id(TypedValueEncodingField::Int);
        test_type_id(TypedValueEncodingField::Integer);
        test_type_id(TypedValueEncodingField::DateTime);
        test_type_id(TypedValueEncodingField::Time);
        test_type_id(TypedValueEncodingField::Date);
        test_type_id(TypedValueEncodingField::Duration);
        test_type_id(TypedValueEncodingField::OtherLiteral);
        test_type_id(TypedValueEncodingField::Null);
    }

    fn test_type_id(term_field: TypedValueEncodingField) {
        assert_eq!(term_field, term_field.type_id().try_into().unwrap());
    }
}
