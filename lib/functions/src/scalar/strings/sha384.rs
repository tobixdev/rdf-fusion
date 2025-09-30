use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{SimpleLiteral, ThinError, TypedValue, TypedValueRef};
use sha2::{Digest, Sha384};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Sha384SparqlOp;

impl Default for Sha384SparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha384SparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Sha384);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for Sha384SparqlOp {
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
            dispatch_unary_owned_typed_value(
                &args.args[0],
                |value| {
                    let string = match value {
                        TypedValueRef::SimpleLiteral(value) => value.value,
                        TypedValueRef::LanguageStringLiteral(value) => value.value,
                        _ => return ThinError::expected(),
                    };

                    let mut hasher = Sha384::new();
                    hasher.update(string);
                    let result = hasher.finalize();
                    let value = format!("{result:x}");

                    Ok(TypedValue::SimpleLiteral(SimpleLiteral { value }))
                },
                ThinError::expected,
            )
        }))
    }
}
