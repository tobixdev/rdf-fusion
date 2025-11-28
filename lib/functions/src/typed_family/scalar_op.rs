use crate::scalar::ScalarSparqlOpArgs;
use crate::scalar::ScalarSparqlOpImpl;
use crate::typed_family::result::TypedFamilyOpResult;
use datafusion::arrow::array::{new_empty_array, Array, ArrayRef, AsArray, UnionArray};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::compute::{interleave, take};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_datafusion_err;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_encoding::typed_value::family::TypedFamilyRef;
use rdf_fusion_encoding::typed_value::{
    TypedValueArray, TypedValueEncoding, TypedValueEncodingField, TypedValueEncodingRef,
};
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::DFResult;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

/// The signature of a single [`TypedFamilySparqlOp`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TypedFamilySparqlOpSignature(Vec<TypedFamilyRef>);

/// A trait for the implementation of a SPARQL operation on a *single* list of typed families.
/// Unary operations will only have a single family, binary operations will have two, etc.
///
/// Multiple implementations (for different families) can be composed together. See
/// [`ExtensibleTypedValueSparqlOp`] for further information on how the dispatching works.
pub trait TypedFamilySparqlOp: Debug + Send + Sync {
    /// Returns the name of the family this operation belongs to.
    fn signature(&self) -> TypedFamilySparqlOpSignature;

    /// Invokes the operation on the given arguments.
    ///
    /// The caller must ensure that the provided arrays all match the data types of the families
    /// provided in [`Self::signature`]. The number of arrays must be the same as the number of
    /// returned families.
    fn invoke(&self, args: &[ArrayRef]) -> DFResult<TypedFamilyOpResult>;
}

/// Represents a composite operation that dispatches to multiple [`TypedFamilySparqlOp`].
///
/// TODO dispatching
///
/// TODO example
///
/// TODO NULL
pub struct ExtensibleTypedValueSparqlOp {
    /// The typed value encoding that is used.
    encoding: TypedValueEncodingRef,
    /// Mapping from a list of type_ids to the operation that is responsible for this list.
    op_mapping: BTreeMap<Vec<u8>, Arc<dyn TypedFamilySparqlOp>>,
}

impl ExtensibleTypedValueSparqlOp {
    /// Creates a new [`ExtensibleTypedValueSparqlOp`] for the given encodings and op mapping.
    pub fn try_new(
        encoding: TypedValueEncodingRef,
        op_mapping: BTreeMap<Vec<TypedFamilyRef>, Arc<dyn TypedFamilySparqlOp>>,
    ) -> Self {
        todo!()
    }
}