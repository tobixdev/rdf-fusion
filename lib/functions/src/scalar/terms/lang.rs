use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{SimpleLiteral, ThinError, TypedValue, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct LangSparqlOp;

impl Default for LangSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl LangSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Lang);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for LangSparqlOp {
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
            dispatch_unary_owned_typed_value(
                &args.args[0],
                |value| {
                    let result = match value {
                        TypedValueRef::NamedNode(_) | TypedValueRef::BlankNode(_) => {
                            return ThinError::expected();
                        }
                        TypedValueRef::LanguageStringLiteral(value) => value.language,
                        _ => "",
                    };
                    Ok(TypedValue::SimpleLiteral(SimpleLiteral {
                        value: result.to_owned(),
                    }))
                },
                ThinError::expected,
            )
        }))
    }
}
