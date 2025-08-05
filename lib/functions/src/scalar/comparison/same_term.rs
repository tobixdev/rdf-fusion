use crate::scalar::dispatch::{dispatch_binary_plain_term, dispatch_binary_typed_value};
use crate::scalar::sparql_op_impl::{
    SparqlOpImpl, create_plain_term_sparql_op_impl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use datafusion::logical_expr::Volatility;
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{LiteralRef, TermRef, ThinError, TypedValueRef};

/// Implementation of the SPARQL `SAME_TERM` operator.
#[derive(Debug)]
pub struct SameTermSparqlOp;

impl Default for SameTermSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl SameTermSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::SameTerm);

    /// Creates a new [SameTermSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for SameTermSparqlOp {
    type Args<TEncoding: TermEncoding> = BinaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn plain_term_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<PlainTermEncoding>>>> {
        Some(create_plain_term_sparql_op_impl(|BinaryArgs(lhs, rhs)| {
            dispatch_binary_plain_term(
                &lhs,
                &rhs,
                |lhs_value, rhs_value| {
                    let value = if lhs_value == rhs_value {
                        "true"
                    } else {
                        "false"
                    };
                    Ok(TermRef::Literal(LiteralRef::new_typed_literal(
                        value,
                        xsd::BOOLEAN,
                    )))
                },
                |_, _| ThinError::expected(),
            )
        }))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        // Comparing two typed-value encoded terms via SAME_TERM is fine. We just have to make sure
        // that pairs like (TypedValue, PlainTerm) do not get converted to TypedValue, but to
        // PlainTerm. Once both terms are in the TypedValue encoding, equality implies same term.
        Some(create_typed_value_sparql_op_impl(|BinaryArgs(lhs, rhs)| {
            dispatch_binary_typed_value(
                &lhs,
                &rhs,
                |lhs_value, rhs_value| {
                    Ok(TypedValueRef::BooleanLiteral(
                        (lhs_value == rhs_value).into(),
                    ))
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
