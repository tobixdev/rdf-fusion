use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::common::ScalarValue;
use rdf_fusion_model::{DFResult, NamedNode, TermRef};
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::Arc;

mod boolean;
mod date_time;
mod duration;
mod numeric;
mod resource;
mod string;
mod unknown;

pub use boolean::*;
pub use date_time::*;
pub use duration::*;
pub use numeric::*;
pub use resource::*;
pub use string::*;
pub use unknown::*;

/// Creates a struct scalar from the given values and fields.
pub fn create_struct_scalar(
    values: Vec<ScalarValue>,
    fields: Fields,
) -> DFResult<ScalarValue> {
    let arrays = values
        .iter()
        .map(|v| v.to_array())
        .collect::<DFResult<Vec<_>>>()?;
    let struct_array =
        datafusion::arrow::array::StructArray::try_new(fields, arrays, None)?;
    Ok(ScalarValue::Struct(Arc::new(struct_array)))
}

/// A cheaply clonable reference to a [`TypeFamily`].
pub type TypeFamilyRef = Arc<dyn TypeFamily>;

/// A [`TypeFamily`] claims types for which to be responsible for.
///
/// For example, the [`BooleanFamily`] claims to be responsible for `xsd:boolean` values, while the
/// [`NumericFamily`] claims `xsd:integer`, `xsd:float`, etc.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TypeClaim {
    /// Claims the responsibility for IRIs and blank node identifiers.
    Resources,
    /// A claim for a set of literal types.
    Literal(BTreeSet<NamedNode>),
    /// A claim for all literal types that are not covered by other claims.
    UnknownLiterals,
}

/// A type family groups together values of related types. Each family defines the encoding
/// of its types within the [`TypedValueEncoding`](crate::encoding::TypedValueEncoding).
///
/// For example, the `xsd:integer`, `xsd:float`, `xsd:int` types belong to the [`NumericFamily`]
/// family. Another example is the `xsd:string` and the `xsd:langString` string types, which belong
/// to the [`StringFamily`]. In addition to the typed values of literals, there is a
/// [`ResourceFamily`] that stores IRIs and blank node identifiers. Lastly, there is a catch-all
/// [`UnknownLiteralFamily`] that stores all unknown literal values.
///
/// Each type family "claims" the types that it is responsible for. See [`TypeClaim`]. This
/// ensures that the typed families partition the set of all possible values correctly. In other
/// words, there is no ambiguity for which family is responsible for a given type.
///
/// Typed Families serve two main purposes:
/// - They allow an *extensible architecture* of the [`TypedValueEncoding`](crate::encoding::TypedValueEncoding),
///   as extensions can add new families. For example, a GeoSPARQL would add a family or
///   multiple families for geospatial types.
/// - It is the *main level of dispatch* for UDFs for the default implementation of operations on
///   the Typed Value Encoding. The first thing that the default implementation does is to check
///   whether all values are from a single family. If this is the case, the entire batch will be
///   directly processed by the specialized implementation of the operation for that family. As each
///   family dictates the Arrow representation of its values, it can be optimized for the data types
///   that are part of this family.
///
/// # Null Handling
///
/// Null values are handled centralized in the typed values encodings unions array. All typed values
/// that are stored directly inside the typed value family *must not be null*. Otherwise, some
/// operations will return incorrect results. Note that parts of the typed value are allowed to be
/// null, just not the whole typed value. For example, a language-tagged string can have a null
/// language tag, but the entire string value can never be null.
pub trait TypeFamily: Debug + Send + Sync {
    /// The id of the typed value family. The id will be used as an extension type name in the
    /// respective union field. As a result, this id must be unique across all typed value
    /// families and must not change between invocations. Therefore, please use a name-space-like
    /// identifier to avoid collisions. For example, `rdf-fusion.resources`.
    ///
    /// The equality of two type families is based on comparing their id.
    fn id(&self) -> &str;

    /// Returns the data type that is used to encode the values of this type family.
    fn data_type(&self) -> &DataType;

    /// Returns the set of claims of this type family. This indicates for which types this family
    /// is responsible. See [`TypeClaim`] for further information.
    fn claim(&self) -> &TypeClaim;

    /// Encodes the given `value` into a [`ScalarValue`].
    fn encode_value(&self, value: TermRef<'_>) -> DFResult<ScalarValue>;
}
