use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use datafusion::logical_expr::Volatility;
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{Float, Numeric, ThinError, TypedValueRef};

#[derive(Debug)]
pub struct CastFloatSparqlOp;

impl Default for CastFloatSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastFloatSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastFloat);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastFloatSparqlOp {
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
                        TypedValueRef::BooleanLiteral(v) => Float::from(v),
                        TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
                        TypedValueRef::NumericLiteral(numeric) => match numeric {
                            Numeric::Int(v) => Float::from(v),
                            Numeric::Integer(v) => Float::from(v),
                            Numeric::Float(v) => v,
                            Numeric::Double(v) => Float::from(v),
                            Numeric::Decimal(v) => Float::from(v),
                        },
                        _ => return ThinError::expected(),
                    };
                    Ok(TypedValueRef::NumericLiteral(Numeric::from(converted)))
                },
                ThinError::expected,
            )
        }))
    }
}
