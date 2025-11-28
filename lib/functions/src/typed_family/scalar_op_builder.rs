use crate::scalar::TypedFamilySparqlOp;
use rdf_fusion_encoding::typed_value::family::TypedFamilyRef;
use rdf_fusion_encoding::typed_value::TypedValueEncodingRef;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A builder for [`ExtensibleTypedValueSparqlOp`]s.
pub struct ExtensibleTypedValueSparqlOpBuilder {
    /// The employed encoding.
    encoding: TypedValueEncodingRef,
    /// The mapping from a list of type_ids to the operation that is responsible for this list.
    op_mapping: BTreeMap<Vec<TypedFamilyRef>, Arc<dyn TypedFamilySparqlOp>>,
}


