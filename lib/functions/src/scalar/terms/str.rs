use rdf_fusion_model::{LiteralRef, TermRef, ThinError};
use rdf_fusion_model::{SimpleLiteral, TypedValue, TypedValueRef};

use crate::builtin::BuiltinName;
use crate::scalar::dispatch::{dispatch_unary_owned_typed_value, dispatch_unary_plain_term};
use crate::scalar::sparql_op_impl::{
    create_plain_term_sparql_op_impl, create_typed_value_sparql_op_impl, SparqlOpImpl,
};
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use crate::FunctionName;
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;

#[derive(Debug)]
pub struct StrSparqlOp;

impl Default for StrSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Str);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrSparqlOp {
    type Args<TEncoding: TermEncoding> = UnaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(|UnaryArgs(arg)| {
            dispatch_unary_owned_typed_value(
                &arg,
                |value| {
                    let converted = match value {
                        TypedValueRef::NamedNode(value) => value.as_str().to_owned(),
                        TypedValueRef::BlankNode(value) => value.as_str().to_owned(),
                        TypedValueRef::BooleanLiteral(value) => value.to_string(),
                        TypedValueRef::NumericLiteral(value) => value.format_value(),
                        TypedValueRef::SimpleLiteral(value) => value.value.to_owned(),
                        TypedValueRef::LanguageStringLiteral(value) => value.value.to_owned(),
                        TypedValueRef::DateTimeLiteral(value) => value.to_string(),
                        TypedValueRef::TimeLiteral(value) => value.to_string(),
                        TypedValueRef::DateLiteral(value) => value.to_string(),
                        TypedValueRef::DurationLiteral(value) => value.to_string(),
                        TypedValueRef::YearMonthDurationLiteral(value) => value.to_string(),
                        TypedValueRef::DayTimeDurationLiteral(value) => value.to_string(),
                        TypedValueRef::OtherLiteral(value) => value.value().to_owned(),
                    };
                    Ok(TypedValue::SimpleLiteral(SimpleLiteral {
                        value: converted,
                    }))
                },
                ThinError::expected,
            )
        }))
    }

    fn plain_term_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<PlainTermEncoding>>>> {
        Some(create_plain_term_sparql_op_impl(|UnaryArgs(arg)| {
            dispatch_unary_plain_term(
                &arg,
                |value| {
                    let converted = match value {
                        TermRef::NamedNode(value) => value.as_str(),
                        TermRef::BlankNode(value) => value.as_str(),
                        TermRef::Literal(value) => value.value(),
                    };
                    Ok(TermRef::Literal(LiteralRef::new_simple_literal(converted)))
                },
                ThinError::expected,
            )
        }))
    }
}
