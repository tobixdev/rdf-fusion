use crate::scalar::dispatch::dispatch_binary_owned_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{LanguageString, ThinError, TypedValue, TypedValueRef};

/// TODO
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct StrLangSparqlOp;

impl Default for StrLangSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrLangSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::StrLang);

    /// Creates a new [StrLangSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrLangSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails::default_with_arity(SparqlOpArity::Fixed(2))
    }
    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_binary_owned_typed_value(
                &args.args[0],
                &args.args[1],
                |lhs_value, rhs_value| {
                    if let (
                        TypedValueRef::SimpleLiteral(lhs_literal),
                        TypedValueRef::SimpleLiteral(rhs_literal),
                    ) = (lhs_value, rhs_value)
                    {
                        Ok(TypedValue::LanguageStringLiteral(LanguageString {
                            value: lhs_literal.value.to_owned(),
                            language: rhs_literal.value.to_ascii_lowercase(),
                        }))
                    } else {
                        ThinError::expected()
                    }
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
