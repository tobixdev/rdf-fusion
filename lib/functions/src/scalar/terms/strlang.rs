use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_binary_owned_typed_value;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::{LanguageString, ThinError, TypedValue, TypedValueRef};

/// TODO
#[derive(Debug)]
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
            dispatch_binary_owned_typed_value(
                &lhs,
                &rhs,
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
