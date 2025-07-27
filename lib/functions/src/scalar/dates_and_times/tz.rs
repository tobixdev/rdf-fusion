use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use datafusion::logical_expr::Volatility;
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{SimpleLiteral, ThinError, TypedValue, TypedValueRef};

#[derive(Debug)]
pub struct TzSparqlOp;

impl Default for TzSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl TzSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Tz);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for TzSparqlOp {
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
            dispatch_unary_owned_typed_value(
                &arg,
                |value| {
                    let tz = match value {
                        TypedValueRef::DateTimeLiteral(v) => v
                            .timezone_offset()
                            .map(|offset| offset.to_string())
                            .unwrap_or_default(),
                        _ => return ThinError::expected(),
                    };
                    Ok(TypedValue::SimpleLiteral(SimpleLiteral { value: tz }))
                },
                ThinError::expected,
            )
        }))
    }
}
