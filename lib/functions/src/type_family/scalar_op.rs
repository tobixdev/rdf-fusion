use crate::type_family::result::TypedFamilyOpResult;
use datafusion::arrow::array::ArrayRef;
use rdf_fusion_encoding::typed_value::family::TypeFamilyRef;
use rdf_fusion_encoding::typed_value::TypedValueEncodingRef;
use rdf_fusion_model::DFResult;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

/// The signature of a single [`TypeFamilyOp`], which builds itself from the ids of the input
/// type families.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TypeFamilyOpSignature(Vec<String>);

impl TypeFamilyOpSignature {
    /// The inner type family ids. For example, the signature
    /// `["rdf-fusion.numeric", "rdf-fusion.string"]` means that the first argument must be from the
    /// `rdf-fusion.numeric` family and the second argument must be from the `rdf-fusion.string`
    /// family.
    pub fn inner(&self) -> &[String] {
        &self.0
    }
}

/// A reference-counted pointer to a [`TypeFamilyOp`].
pub type TypeFamilyOpRef = Arc<dyn TypeFamilyOp>;

/// A trait for the implementation of a SPARQL operation on a *single* list of typed families.
/// Unary operations will only have a single family, binary operations will have two, etc.
///
/// Multiple implementations (for different families) can be composed together. See
/// [`ExtensibleTypedValueSparqlOp`] for further information on how the dispatching works.
pub trait TypeFamilyOp: Debug + Send + Sync {
    /// Returns the name of the family this operation belongs to.
    fn signature(&self) -> TypeFamilyOpSignature;

    /// Invokes the operation on the given arguments.
    ///
    /// The caller must ensure that the provided arrays all match the data types of the families
    /// provided in [`Self::signature`]. The number of arrays must be the same as the number of
    /// returned families.
    fn invoke(&self, args: &[ArrayRef]) -> DFResult<TypedFamilyOpResult>;
}

/// Represents a composite operation that dispatches to multiple [`TypeFamilyOp`].
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
    op_mapping: BTreeMap<Vec<u8>, Arc<dyn TypeFamilyOp>>,
}

impl ExtensibleTypedValueSparqlOp {
    /// Creates a new [`ExtensibleTypedValueSparqlOp`] for the given encodings and op mapping.
    pub fn try_new(
        encoding: TypedValueEncodingRef,
        op_mapping: BTreeMap<Vec<TypeFamilyRef>, Arc<dyn TypeFamilyOp>>,
    ) -> Self {
        todo!()
    }

    /// Returns the op mappings that constitute this operation.
    pub fn op_mapping(&self) -> &BTreeMap<Vec<TypeFamilyRef>, Arc<dyn TypeFamilyOp>> {
        todo!("Extract mapping")
    }
}
