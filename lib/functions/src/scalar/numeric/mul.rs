use rdf_fusion_api::functions::BuiltinName;
use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use rdf_fusion_api::functions::FunctionName;
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::{Numeric, NumericPair, ThinError, TypedValueRef};

/// Implementation of the SPARQL `*` operator.
#[derive(Debug)]
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
    type Args<TEncoding: TermEncoding> = BinaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(|BinaryArgs(lhs, rhs)| {
            dispatch_binary_typed_value(
                &lhs,
                &rhs,
                |lhs_value, rhs_value| {
                    if let (
                        TypedValueRef::NumericLiteral(lhs_numeric),
                        TypedValueRef::NumericLiteral(rhs_numeric),
                    ) = (lhs_value, rhs_value)
                    {
                        match NumericPair::with_casts_from(lhs_numeric, rhs_numeric) {
                            NumericPair::Int(lhs, rhs) => lhs.checked_mul(rhs).map(Numeric::Int),
                            NumericPair::Integer(lhs, rhs) => {
                                lhs.checked_mul(rhs).map(Numeric::Integer)
                            }
                            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs * rhs)),
                            NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs * rhs)),
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
