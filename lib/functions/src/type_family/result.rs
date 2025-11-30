use crate::type_family::error::TypedFamilyPureResultError;
use datafusion::arrow::array::{Array, ArrayRef};
use rdf_fusion_encoding::typed_value::family::TypeFamilyRef;
use rdf_fusion_encoding::typed_value::TypedValueArray;

/// The result of a family-specific operation. This will be returned when invoking the typed value
/// operation on the subset of a specific typed value family.
pub enum TypedFamilyOpResult {
    /// The result is a single array that maps to a specific field in the `TypedValueEncoding`.
    Pure(TypedFamilyPureResult),
    /// The result is a full `TypedValueArray` (mixed types).
    Mixed(TypedValueArray),
}

/// A struct representing a pure result of a type family operation. It has already been validated
/// that the contained array is conformant to the type family.
///
/// # Null Handling
///
/// The array that contains the result *can* contain null values. This is contrary to the
/// requirement that no type family (see [`TypeFamily`]) is allowed to store null values. Null
/// values contained in this array will be automatically transformed into the global null array by
/// the [`ExtensibleTypedValueSparqlOp`] and filtered out.
pub struct TypedFamilyPureResult {
    /// The typed value family that produced this result.
    typed_family: TypeFamilyRef,
    /// The array that contains the result.
    array: ArrayRef,
}

impl TypedFamilyPureResult {
    /// Tries to create a new [`TypedFamilyPureResult`], validating that the array is conformant to
    /// the type family.
    pub fn try_new(
        typed_family: TypeFamilyRef,
        array: ArrayRef,
    ) -> Result<Self, TypedFamilyPureResultError> {
        if typed_family.data_type() != array.data_type() {
            return Err(TypedFamilyPureResultError::DataTypeDoesNotMatch(
                typed_family.data_type().clone(),
                array.data_type().clone(),
            ));
        }

        Ok(Self {
            typed_family,
            array,
        })
    }
}
