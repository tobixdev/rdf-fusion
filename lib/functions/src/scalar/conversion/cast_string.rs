use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_model::ThinError;
use rdf_fusion_model::{SimpleLiteral, TypedValue, TypedValueRef};

use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CastStringSparqlOp;

impl Default for CastStringSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastStringSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastString);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastStringSparqlOp {
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
                        let converted = match value {
                            TypedValueRef::NamedNode(value) => value.as_str().to_owned(),
                            TypedValueRef::BlankNode(_) => return ThinError::expected(),
                            TypedValueRef::BooleanLiteral(value) => value.to_string(),
                            TypedValueRef::NumericLiteral(value) => value.format_value(),
                            TypedValueRef::SimpleLiteral(value) => value.value.to_owned(),
                            TypedValueRef::LanguageStringLiteral(value) => {
                                value.value.to_owned()
                            }
                            TypedValueRef::DateTimeLiteral(value) => value.to_string(),
                            TypedValueRef::TimeLiteral(value) => value.to_string(),
                            TypedValueRef::DateLiteral(value) => value.to_string(),
                            TypedValueRef::DurationLiteral(value) => value.to_string(),
                            TypedValueRef::YearMonthDurationLiteral(value) => {
                                value.to_string()
                            }
                            TypedValueRef::DayTimeDurationLiteral(value) => {
                                value.to_string()
                            }
                            TypedValueRef::OtherLiteral(value) => {
                                value.value().to_owned()
                            }
                        };
                        Ok(TypedValue::SimpleLiteral(SimpleLiteral {
                            value: converted,
                        }))
                    },
                    ThinError::expected,
                )
            },
        ))
    }
}
