use crate::scalar::dispatch::dispatch_binary_owned_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{LanguageString, ThinError, TypedValue, TypedValueRef};

/// Creates a new RDF literal from a plain literal and a language tag.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - STRLANG](https://www.w3.org/TR/sparql11-query/#func-strlang)
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

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(2))
    }

    fn typed_value_encoding_op(
        &self,
        encodings: &RdfFusionEncodings,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(
            encodings.typed_value(),
            |args| {
                dispatch_binary_owned_typed_value(
                    &args.encoding,
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
            },
        ))
    }
}
