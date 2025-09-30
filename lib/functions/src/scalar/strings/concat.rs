use crate::scalar::dispatch::dispatch_n_ary_owned_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{
    LanguageString, SimpleLiteral, StringLiteralRef, ThinError, ThinResult, TypedValue,
};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ConcatSparqlOp;

impl Default for ConcatSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcatSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Concat);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for ConcatSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Variadic)
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_n_ary_owned_typed_value(
                args.args.as_slice(),
                args.number_rows,
                |args| {
                    let args = args
                        .iter()
                        .map(|arg| StringLiteralRef::try_from(*arg))
                        .collect::<ThinResult<Vec<_>>>()?;

                    let mut result = String::default();
                    let mut language = None;

                    for arg in args {
                        if let Some(lang) = &language {
                            if *lang != arg.1 {
                                language = Some(None)
                            }
                        } else {
                            language = Some(arg.1)
                        }
                        result += arg.0;
                    }

                    Ok(match language.flatten().map(ToOwned::to_owned) {
                        Some(language) => {
                            TypedValue::LanguageStringLiteral(LanguageString {
                                value: result,
                                language,
                            })
                        }
                        None => {
                            TypedValue::SimpleLiteral(SimpleLiteral { value: result })
                        }
                    })
                },
                |_| ThinError::expected(),
            )
        }))
    }
}
