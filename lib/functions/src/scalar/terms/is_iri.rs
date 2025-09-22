use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{ThinError, TypedValueRef};

/// TODO
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct IsIriSparqlOp;

impl Default for IsIriSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsIriSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::IsIri);

    /// Creates a new [IsIriSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for IsIriSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails::default_with_arity(SparqlOpArity::Fixed(1))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_unary_typed_value(
                &args.args[0],
                |value| {
                    Ok(TypedValueRef::BooleanLiteral(
                        matches!(value, TypedValueRef::NamedNode(_)).into(),
                    ))
                },
                ThinError::expected,
            )
        }))
    }
}
