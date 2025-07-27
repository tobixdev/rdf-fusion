use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use datafusion::logical_expr::Volatility;
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{Numeric, NumericPair, ThinError, TypedValueRef};

/// Implementation of the SPARQL `+` operator.
#[derive(Debug)]
pub struct AddSparqlOp;

impl Default for AddSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AddSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Add);

    /// Creates a new [AddSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for AddSparqlOp {
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
                        let result = match NumericPair::with_casts_from(
                            lhs_numeric,
                            rhs_numeric,
                        ) {
                            NumericPair::Int(lhs, rhs) => {
                                lhs.checked_add(rhs).map(Numeric::Int)
                            }
                            NumericPair::Integer(lhs, rhs) => {
                                lhs.checked_add(rhs).map(Numeric::Integer)
                            }
                            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs + rhs)),
                            NumericPair::Double(lhs, rhs) => {
                                Ok(Numeric::Double(lhs + rhs))
                            }
                            NumericPair::Decimal(lhs, rhs) => {
                                lhs.checked_add(rhs).map(Numeric::Decimal)
                            }
                        }?;
                        Ok(TypedValueRef::NumericLiteral(result))
                    } else {
                        ThinError::expected()
                    }
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
