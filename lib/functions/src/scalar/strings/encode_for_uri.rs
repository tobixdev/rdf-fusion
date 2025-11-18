use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{SimpleLiteral, ThinError, TypedValue, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct EncodeForUriSparqlOp;

impl Default for EncodeForUriSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl EncodeForUriSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::EncodeForUri);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for EncodeForUriSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(1))
    }

    fn typed_value_encoding_op(
        &self,
        encodings: &RdfFusionEncodings,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(
            encodings.typed_value(),
            |args| {
                dispatch_unary_owned_typed_value(
                    &args.encoding,
                    &args.args[0],
                    |value| {
                        let string = match value {
                            TypedValueRef::SimpleLiteral(value) => value.value,
                            TypedValueRef::LanguageStringLiteral(value) => value.value,
                            _ => return ThinError::expected(),
                        };

                        // Based on oxigraph/lib/spareval/src/eval.rs
                        // Maybe we can use a library in the future?
                        let mut result = Vec::with_capacity(string.len());
                        for c in string.bytes() {
                            match c {
                                b'A'..=b'Z'
                                | b'a'..=b'z'
                                | b'0'..=b'9'
                                | b'-'
                                | b'_'
                                | b'.'
                                | b'~' => result.push(c),
                                _ => {
                                    result.push(b'%');
                                    let high = c / 16;
                                    let low = c % 16;
                                    result.push(if high < 10 {
                                        b'0' + high
                                    } else {
                                        b'A' + (high - 10)
                                    });
                                    result.push(if low < 10 {
                                        b'0' + low
                                    } else {
                                        b'A' + (low - 10)
                                    });
                                }
                            }
                        }

                        let value = String::from_utf8(result)?;
                        Ok(TypedValue::SimpleLiteral(SimpleLiteral { value }))
                    },
                    ThinError::expected,
                )
            },
        ))
    }
}
