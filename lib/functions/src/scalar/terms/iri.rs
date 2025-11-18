use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{
    ScalarSparqlOp, ScalarSparqlOpArgs, ScalarSparqlOpSignature, SparqlOpArity,
};
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::{EncodingDatum, RdfFusionEncodings, TermDecoder};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{Iri, NamedNode, ThinError, TypedValue, TypedValueRef};

#[derive(Debug, Default, Hash, PartialEq, Eq)]
pub struct IriSparqlOp;

impl IriSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Iri);

    fn get_base_iri(
        args: &ScalarSparqlOpArgs<TypedValueEncoding>,
    ) -> DFResult<Option<Iri<String>>> {
        match &args.args[1] {
            EncodingDatum::Array(_) => {
                exec_err!("IRI does not support a scalar base_iri")
            }
            EncodingDatum::Scalar(value, _) => {
                let term = DefaultTypedValueDecoder::decode_term(value).ok();
                term.map(|t| match t {
                    TypedValueRef::SimpleLiteral(lit) => Iri::parse(lit.value.to_owned())
                        .map_err(|_| exec_datafusion_err!("Invalid IRI: {}", lit.value)),
                    _ => exec_err!("Unexpected typed value for base_iri."),
                })
                .transpose()
            }
        }
    }
}

impl ScalarSparqlOp for IriSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature {
            volatility: Volatility::Immutable,
            arity: SparqlOpArity::Fixed(2),
        }
    }

    fn typed_value_encoding_op(
        &self,
        encodings: &RdfFusionEncodings,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(
            encodings.typed_value(),
            |args| {
                let base_iri = Self::get_base_iri(&args)?;
                dispatch_unary_owned_typed_value(
                    &args.encoding,
                    &args.args[0],
                    |value| match value {
                        TypedValueRef::NamedNode(named_node) => {
                            Ok(TypedValue::NamedNode(named_node.into_owned()))
                        }
                        TypedValueRef::SimpleLiteral(simple_literal) => {
                            let resolving_result = if let Some(base_iri) = &base_iri {
                                base_iri.resolve(simple_literal.value)?
                            } else {
                                Iri::parse(simple_literal.value.to_owned())?
                            };
                            Ok(TypedValue::NamedNode(NamedNode::from(resolving_result)))
                        }
                        _ => ThinError::expected(),
                    },
                    ThinError::expected,
                )
            },
        ))
    }
}
