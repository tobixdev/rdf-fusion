use crate::type_family::{
    ExtensibleTypedValueSparqlOp, ExtensibleTypedValueSparqlOpCreationError,
    TypeFamilyOp, TypeFamilyOpRef, TypeFamilyOpSignature,
};
use datafusion::logical_expr::ScalarUDF;
use rdf_fusion_encoding::typed_value::TypedValueEncodingRef;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A builder for [`ExtensibleTypedValueSparqlOp`]s.
pub struct ExtensibleTypedValueSparqlOpBuilder {
    /// The employed encoding.
    encoding: TypedValueEncodingRef,
    /// The mapping from a list of type_ids to the operation that is responsible for this list.
    op_mapping: BTreeMap<TypeFamilyOpSignature, Arc<dyn TypeFamilyOp>>,
}

impl ExtensibleTypedValueSparqlOpBuilder {
    /// Creates a new [`ExtensibleTypedValueSparqlOpBuilder`] for the given encoding.
    pub fn new(encoding: TypedValueEncodingRef) -> Self {
        Self {
            encoding,
            op_mapping: BTreeMap::new(),
        }
    }

    /// Tries to create a new builder from a UDF by trying to extrac the type family mapping from
    /// the UDF signature.
    ///
    /// This only works for functions that are implemented using an [`ExtensibleTypedValueSparqlOp`].
    /// Otherwise, an error will be returned.
    pub fn try_from_udf(
        encoding: TypedValueEncodingRef,
        udf: &ScalarUDF,
    ) -> Result<Self, ExtensibleTypedValueSparqlOpCreationError> {
        let inner = udf.inner().as_any().downcast_ref::<ExtensibleTypedValueSparqlOp>()
            .ok_or_else(|| ExtensibleTypedValueSparqlOpCreationError::CannotExtractFamilyMappingFromUDF(udf.name().to_owned()))?;

        let mut builder = Self::new(encoding);
        for (_, op) in inner.op_mapping() {
            builder = builder.with_op(Arc::clone(op))?;
        }

        Ok(builder)
    }

    /// Adds a new [`TypeFamilyOp`] to the SPARQL operation.
    ///
    /// This will override any existing registrations for the given signature.
    pub fn with_op(
        mut self,
        op: TypeFamilyOpRef,
    ) -> Result<Self, ExtensibleTypedValueSparqlOpCreationError> {
        let signature = op.signature();

        let unregistered_tf_id = signature
            .inner()
            .iter()
            .find(|tf_id| self.encoding.find_type_family(tf_id).is_none());
        if let Some(unregistered_tf_id) = unregistered_tf_id {
            return Err(
                ExtensibleTypedValueSparqlOpCreationError::UnknownTypeFamily(
                    unregistered_tf_id.to_owned(),
                ),
            );
        }

        self.op_mapping.insert(signature, op);
        Ok(self)
    }
}
