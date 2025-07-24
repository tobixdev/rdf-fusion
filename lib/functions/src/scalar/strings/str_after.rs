use rdf_fusion_api::functions::BuiltinName;
use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use rdf_fusion_api::functions::FunctionName;
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::{
    CompatibleStringArgs, LanguageStringRef, SimpleLiteralRef, StringLiteralRef, ThinError,
    TypedValueRef,
};

/// Implementation of the SPARQL `strafter` function.
#[derive(Debug)]
pub struct StrAfterSparqlOp;

impl Default for StrAfterSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrAfterSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::StrAfter);

    /// Creates a new [StrAfterSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrAfterSparqlOp {
    type Args<TEncoding: TermEncoding> = BinaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(|BinaryArgs(lhs, rhs)| {
            dispatch_binary_typed_value(
                &lhs,
                &rhs,
                |lhs_value, rhs_value| {
                    let lhs_value = StringLiteralRef::try_from(lhs_value)?;
                    let rhs_value = StringLiteralRef::try_from(rhs_value)?;

                    let args = CompatibleStringArgs::try_from(lhs_value, rhs_value)?;

                    let value = if let Some(position) = args.lhs.find(args.rhs) {
                        let start = position + args.rhs.len();
                        &args.lhs[start..]
                    } else {
                        return Ok(TypedValueRef::SimpleLiteral(SimpleLiteralRef { value: "" }));
                    };

                    Ok(match args.language {
                        None => TypedValueRef::SimpleLiteral(SimpleLiteralRef { value }),
                        Some(language) => TypedValueRef::LanguageStringLiteral(LanguageStringRef {
                            value,
                            language,
                        }),
                    })
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
