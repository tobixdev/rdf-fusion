use crate::encoding::TermEncoding;
use crate::typed_value::array::TypedValueArray;
use crate::typed_value::error::TypedValueEncodingCreationError;
use crate::typed_value::family::{
    BooleanFamily, DateTimeFamily, DurationFamily, NumericFamily, ResourceFamily,
    StringFamily, TypeFamilyRef, UnknownFamily,
};
use crate::typed_value::scalar::TypedValueScalar;
use crate::EncodingName;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use datafusion::common::ScalarValue;
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{TermRef, ThinResult};
use std::clone::Clone;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

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
    /// The registered families (Resource + others + Unknown).
    families: Vec<TypeFamilyRef>,
    /// The registered resource family.
    resource_family: TypeFamilyRef,
    /// The registered unknown family.
    unknown_family: TypeFamilyRef,
}

impl TypedValueEncoding {
    /// The type id of the null column
    pub const NULL_TYPE_ID: i8 = 0;

    /// Creates a new [`TypedValueEncoding`].
    ///
    /// # Errors
    ///
    /// Returns an error if ...
    /// - more than one type families with the same id are provided.
    /// - the set of claimed types overlaps between two families.
    pub fn try_new(
        resource_family: TypeFamilyRef,
        type_families: Vec<TypeFamilyRef>,
        unknown_family: TypeFamilyRef,
    ) -> Result<Self, TypedValueEncodingCreationError> {
        let mut families = Vec::with_capacity(type_families.len() + 2);
        families.push(resource_family.clone());
        families.extend(type_families);
        families.push(unknown_family.clone());

        let mut seen = HashSet::new();
        for type_family in &families {
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
            data_type: build_data_type(&families),
            families,
            resource_family,
            unknown_family,
        })
    }

    /// Returns the fields of the data type of this encoding.
    pub fn data_type_fields(&self) -> UnionFields {
        match &self.data_type {
            DataType::Union(fields, _) => fields.clone(),
            _ => unreachable!("Always a union array"),
        }
    }

    /// Returns a reference to the registered type families.
    ///
    /// The slice will be in the same order as the type families are encoded in the union array.
    pub fn type_families(&self) -> &[TypeFamilyRef] {
        &self.families
    }

    /// Returns the resource family.
    pub fn resource_family(&self) -> &TypeFamilyRef {
        &self.resource_family
    }

    /// Returns the unknown family.
    pub fn unknown_family(&self) -> &TypeFamilyRef {
        &self.unknown_family
    }

    /// Returns the number of registered type families.
    ///
    /// Note that this does not include the null array.
    pub fn num_type_families(&self) -> usize {
        self.families.len()
    }

    /// Tries to find a registered [`TypeFamilyRef`] with the given name.
    pub fn find_type_family(&self, id: &str) -> Option<(i8, &TypeFamilyRef)> {
        self.families
            .iter()
            .enumerate()
            .find(|(_, f)| f.id() == id)
            .map(|(i, f)| ((i + 1) as i8, f))
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
        UnionFields::new(0..fields.len() as i8, fields),
        UnionMode::Dense,
    )
}

impl Default for TypedValueEncoding {
    fn default() -> Self {
        let families: Vec<TypeFamilyRef> = vec![
            Arc::new(StringFamily::new()),
            Arc::new(BooleanFamily::new()),
            Arc::new(NumericFamily::new()),
            Arc::new(DateTimeFamily::new()),
            Arc::new(DurationFamily::new()),
        ];
        Self::try_new(
            Arc::new(ResourceFamily::new()),
            families,
            Arc::new(UnknownFamily::new()),
        )
        .unwrap()
    }
}

impl TypedValueEncoding {
    /// Encodes the `term` as a [TypedValueScalar].
    pub fn encode_term(
        &self,
        term: ThinResult<TermRef<'_>>,
    ) -> DFResult<TypedValueScalar> {
        // let arc = Arc::new(self.clone());
        // TermRefTypedValueEncoder::new(arc)
        //     .encode_terms([term])?
        //     .try_as_scalar(0)
        todo!("Use builder and try_as_scalar")
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
