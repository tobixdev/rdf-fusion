use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use datafusion::logical_expr::Volatility;
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{
    CompatibleStringArgs, StringLiteralRef, ThinError, TypedValueRef,
};

/// Implementation of the SPARQL `strends` function.
#[derive(Debug)]
pub struct StrEndsSparqlOp;

impl Default for StrEndsSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrEndsSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::StrEnds);

    /// Creates a new [StrEndsSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrEndsSparqlOp {
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
                    let lhs_value = StringLiteralRef::try_from(lhs_value)?;
                    let rhs_value = StringLiteralRef::try_from(rhs_value)?;
                    let args = CompatibleStringArgs::try_from(lhs_value, rhs_value)?;
                    Ok(TypedValueRef::BooleanLiteral(
                        args.lhs.ends_with(args.rhs).into(),
                    ))
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
