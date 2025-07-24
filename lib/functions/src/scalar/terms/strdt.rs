use rdf_fusion_api::functions::BuiltinName;
use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use rdf_fusion_api::functions::FunctionName;
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::{LiteralRef, ThinError, TypedValueRef};

/// TODO
#[derive(Debug)]
pub struct StrDtSparqlOp;

impl Default for StrDtSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrDtSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::StrDt);

    /// Creates a new [StrDtSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrDtSparqlOp {
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
                        TypedValueRef::SimpleLiteral(lhs_literal),
                        TypedValueRef::NamedNode(rhs_named_node),
                    ) = (lhs_value, rhs_value)
                    {
                        let plain_literal =
                            LiteralRef::new_typed_literal(lhs_literal.value, rhs_named_node);
                        TypedValueRef::try_from(plain_literal).map_err(|_| ThinError::Expected)
                    } else {
                        ThinError::expected()
                    }
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
