use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use datafusion::logical_expr::Volatility;
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{Integer, Numeric, ThinError, TypedValueRef};

#[derive(Debug)]
pub struct CastIntegerSparqlOp;

impl Default for CastIntegerSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastIntegerSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastInteger);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastIntegerSparqlOp {
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
                        TypedValueRef::BooleanLiteral(v) => Integer::from(v),
                        TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
                        TypedValueRef::NumericLiteral(numeric) => match numeric {
                            Numeric::Int(v) => Integer::from(v),
                            Numeric::Integer(v) => v,
                            Numeric::Float(v) => Integer::try_from(v)?,
                            Numeric::Double(v) => Integer::try_from(v)?,
                            Numeric::Decimal(v) => Integer::try_from(v)?,
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
