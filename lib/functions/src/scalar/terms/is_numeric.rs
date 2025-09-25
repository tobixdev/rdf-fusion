use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct IsNumericSparqlOp;

impl Default for IsNumericSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsNumericSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::IsNumeric);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for IsNumericSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(1))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_unary_typed_value(
                &args.args[0],
                |value| {
                    Ok(TypedValueRef::BooleanLiteral(
                        matches!(value, TypedValueRef::NumericLiteral(_)).into(),
                    ))
                },
                ThinError::expected,
            )
        }))
    }
}
