use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use crate::FunctionName;
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::{Numeric, ThinError, TypedValueRef};

#[derive(Debug)]
pub struct RoundSparqlOp;

impl Default for RoundSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Round);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for RoundSparqlOp {
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
                        let result = match numeric {
                            Numeric::Float(v) => Ok(Numeric::Float(v.round())),
                            Numeric::Double(v) => Ok(Numeric::Double(v.round())),
                            Numeric::Decimal(v) => v.checked_round().map(Numeric::Decimal),
                            _ => Ok(numeric),
                        };
                        result.map(TypedValueRef::NumericLiteral)
                    }
                    _ => ThinError::expected(),
                },
                ThinError::expected,
            )
        }))
    }
}
