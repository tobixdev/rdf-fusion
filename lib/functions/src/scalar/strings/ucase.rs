use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{
    LanguageString, SimpleLiteral, ThinError, TypedValue, TypedValueRef,
};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct UCaseSparqlOp;

impl Default for UCaseSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl UCaseSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::UCase);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for UCaseSparqlOp {
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
                |value| match value {
                    TypedValueRef::SimpleLiteral(value) => {
                        Ok(TypedValue::SimpleLiteral(SimpleLiteral {
                            value: value.value.to_uppercase(),
                        }))
                    }
                    TypedValueRef::LanguageStringLiteral(value) => {
                        Ok(TypedValue::LanguageStringLiteral(LanguageString {
                            value: value.value.to_uppercase(),
                            language: value.language.to_owned(),
                        }))
                    }
                    _ => ThinError::expected(),
                },
                ThinError::expected,
            )
        }))
    }
}
