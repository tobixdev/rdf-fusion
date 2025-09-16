use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{Numeric, NumericPair, ThinError, TypedValueRef};

/// Implementation of the SPARQL `*` operator.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct MulSparqlOp;

impl Default for MulSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl MulSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Mul);

    /// Creates a new [MulSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for MulSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails::default_with_arity(SparqlOpArity::Fixed(2))
    }
    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
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
                                lhs.checked_mul(rhs).map(Numeric::Int)
                            }
                            NumericPair::Integer(lhs, rhs) => {
                                lhs.checked_mul(rhs).map(Numeric::Integer)
                            }
                            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs * rhs)),
                            NumericPair::Double(lhs, rhs) => {
                                Ok(Numeric::Double(lhs * rhs))
                            }
                            NumericPair::Decimal(lhs, rhs) => {
                                lhs.checked_mul(rhs).map(Numeric::Decimal)
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
