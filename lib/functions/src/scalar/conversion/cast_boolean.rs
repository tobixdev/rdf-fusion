use rdf_fusion_api::functions::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use rdf_fusion_api::functions::FunctionName;
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::{Boolean, Numeric, ThinError, TypedValueRef};

#[derive(Debug)]
pub struct CastBooleanSparqlOp;

impl Default for CastBooleanSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastBooleanSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastBoolean);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastBooleanSparqlOp {
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
                |value| {
                    let converted = match value {
                        TypedValueRef::BooleanLiteral(v) => v,
                        TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
                        TypedValueRef::NumericLiteral(numeric) => match numeric {
                            Numeric::Int(v) => Boolean::from(v),
                            Numeric::Integer(v) => Boolean::from(v),
                            Numeric::Float(v) => Boolean::from(v),
                            Numeric::Double(v) => Boolean::from(v),
                            Numeric::Decimal(v) => Boolean::from(v),
                        },
                        _ => return ThinError::expected(),
                    };
                    Ok(TypedValueRef::from(converted))
                },
                ThinError::expected,
            )
        }))
    }
}
