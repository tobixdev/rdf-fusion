use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use datafusion::logical_expr::Volatility;
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct UnaryMinusSparqlOp;

impl Default for UnaryMinusSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl UnaryMinusSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::UnaryMinus);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for UnaryMinusSparqlOp {
    type Args<TEncoding: TermEncoding> = UnaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(|UnaryArgs(arg)| {
            dispatch_unary_typed_value(
                &arg,
                |value| match value {
                    TypedValueRef::NumericLiteral(numeric) => {
                        numeric.neg().map(TypedValueRef::NumericLiteral)
                    }
                    _ => ThinError::expected(),
                },
                ThinError::expected,
            )
        }))
    }
}
