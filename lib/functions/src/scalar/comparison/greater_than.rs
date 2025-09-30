use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{ThinError, TypedValueRef};
use std::cmp::Ordering;

/// Implementation of the SPARQL `>` operator.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct GreaterThanSparqlOp;

impl Default for GreaterThanSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl GreaterThanSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::GreaterThan);

    /// Creates a new [GreaterThanSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for GreaterThanSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(2))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_binary_typed_value(
                &args.args[0],
                &args.args[1],
                |lhs_value, rhs_value| {
                    lhs_value
                        .partial_cmp(&rhs_value)
                        .map(|o| o == Ordering::Greater)
                        .map(Into::into)
                        .map(TypedValueRef::BooleanLiteral)
                        .ok_or(ThinError::ExpectedError)
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
