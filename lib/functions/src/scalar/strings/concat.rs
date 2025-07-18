use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_n_ary_owned_typed_value;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{NAryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::{
    LanguageString, SimpleLiteral, StringLiteralRef, ThinError, ThinResult, TypedValue,
};

#[derive(Debug)]
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
    type Args<TEncoding: TermEncoding> = NAryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(
            |NAryArgs(args, number_rows)| {
                dispatch_n_ary_owned_typed_value(
                    &args,
                    number_rows,
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
                            Some(language) => TypedValue::LanguageStringLiteral(LanguageString {
                                value: result,
                                language,
                            }),
                            None => TypedValue::SimpleLiteral(SimpleLiteral { value: result }),
                        })
                    },
                    |_| ThinError::expected(),
                )
            },
        ))
    }
}
