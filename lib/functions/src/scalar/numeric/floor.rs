use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{Numeric, ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct FloorSparqlOp;

impl Default for FloorSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl FloorSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Floor);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for FloorSparqlOp {
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
                |value| match value {
                    TypedValueRef::NumericLiteral(numeric) => {
                        let result = match numeric {
                            Numeric::Float(v) => Ok(Numeric::Float(v.floor())),
                            Numeric::Double(v) => Ok(Numeric::Double(v.floor())),
                            Numeric::Decimal(v) => {
                                v.checked_floor().map(Numeric::Decimal)
                            }
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
