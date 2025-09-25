use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{Decimal, Numeric, NumericPair, ThinError, TypedValueRef};

/// Implementation of the SPARQL `/` operator.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct DivSparqlOp;

impl Default for DivSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl DivSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Div);

    /// Creates a new [DivSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for DivSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(2))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_binary_typed_value(
                &args.args[0],
                &args.args[1],
                |lhs_value, rhs_value| {
                    if let (
                        TypedValueRef::NumericLiteral(lhs_numeric),
                        TypedValueRef::NumericLiteral(rhs_numeric),
                    ) = (lhs_value, rhs_value)
                    {
                        match NumericPair::with_casts_from(lhs_numeric, rhs_numeric) {
                            NumericPair::Int(lhs, rhs) => {
                                Decimal::from(lhs).checked_div(rhs).map(Numeric::Decimal)
                            }
                            NumericPair::Integer(lhs, rhs) => {
                                Decimal::from(lhs).checked_div(rhs).map(Numeric::Decimal)
                            }
                            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs / rhs)),
                            NumericPair::Double(lhs, rhs) => {
                                Ok(Numeric::Double(lhs / rhs))
                            }
                            NumericPair::Decimal(lhs, rhs) => {
                                lhs.checked_div(rhs).map(Numeric::Decimal)
                            }
                        }
                        .map(TypedValueRef::NumericLiteral)
                    } else {
                        ThinError::expected()
                    }
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
