use crate::scalar::dispatch::dispatch_binary_plain_term;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_plain_term_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{LiteralRef, TermRef, ThinError};

/// Implementation of the SPARQL `SAME_TERM` operator.
#[derive(Debug, Hash, PartialEq, Eq)]
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
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(2))
    }

    fn plain_term_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<PlainTermEncoding>>> {
        Some(create_plain_term_sparql_op_impl(|args| {
            dispatch_binary_plain_term(
                &args.args[0],
                &args.args[1],
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
}
