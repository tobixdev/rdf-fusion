use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{Numeric, ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
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
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails::default_with_arity(SparqlOpArity::Fixed(1))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_unary_typed_value(
                &args.args[0],
                |value| match value {
                    TypedValueRef::NumericLiteral(numeric) => {
                        let result = match numeric {
                            Numeric::Float(v) => Ok(Numeric::Float(v.round())),
                            Numeric::Double(v) => Ok(Numeric::Double(v.round())),
                            Numeric::Decimal(v) => {
                                v.checked_round().map(Numeric::Decimal)
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
