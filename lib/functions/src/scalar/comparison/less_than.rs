use rdf_fusion_api::functions::BuiltinName;
use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use rdf_fusion_api::functions::FunctionName;
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::{ThinError, TypedValueRef};
use std::cmp::Ordering;

/// Implementation of the SPARQL `<` operator.
#[derive(Debug)]
pub struct LessThanSparqlOp;

impl Default for LessThanSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl LessThanSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::LessThan);

    /// Creates a new [LessThanSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for LessThanSparqlOp {
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
                    lhs_value
                        .partial_cmp(&rhs_value)
                        .map(|o| o == Ordering::Less)
                        .map(Into::into)
                        .map(TypedValueRef::BooleanLiteral)
                        .ok_or(ThinError::default())
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
