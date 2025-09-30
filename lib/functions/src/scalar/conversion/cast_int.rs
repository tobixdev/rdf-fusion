use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{Int, Numeric, ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CastIntSparqlOp;

impl Default for CastIntSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastIntSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::AsInt);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastIntSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(1))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_unary_typed_value(
                &args.args[0],
                |value| {
                    let converted = match value {
                        TypedValueRef::BooleanLiteral(v) => Int::from(v),
                        TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
                        TypedValueRef::NumericLiteral(numeric) => match numeric {
                            Numeric::Int(v) => v,
                            Numeric::Integer(v) => Int::try_from(v)?,
                            Numeric::Float(v) => Int::try_from(v)?,
                            Numeric::Double(v) => Int::try_from(v)?,
                            Numeric::Decimal(v) => Int::try_from(v)?,
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
