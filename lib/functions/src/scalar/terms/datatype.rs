use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::vocab::{rdf, xsd};
use rdf_fusion_model::{Numeric, ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct DatatypeSparqlOp;

impl Default for DatatypeSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl DatatypeSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Datatype);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for DatatypeSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(1))
    }
    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_unary_typed_value(
                &args.args[0],
                |value| {
                    let iri = match value {
                        TypedValueRef::BlankNode(_) | TypedValueRef::NamedNode(_) => {
                            return ThinError::expected();
                        }
                        TypedValueRef::SimpleLiteral(_) => xsd::STRING,
                        TypedValueRef::NumericLiteral(value) => match value {
                            Numeric::Int(_) => xsd::INT,
                            Numeric::Integer(_) => xsd::INTEGER,
                            Numeric::Float(_) => xsd::FLOAT,
                            Numeric::Double(_) => xsd::DOUBLE,
                            Numeric::Decimal(_) => xsd::DECIMAL,
                        },
                        TypedValueRef::BooleanLiteral(_) => xsd::BOOLEAN,
                        TypedValueRef::LanguageStringLiteral(_) => rdf::LANG_STRING,
                        TypedValueRef::DateTimeLiteral(_) => xsd::DATE_TIME,
                        TypedValueRef::TimeLiteral(_) => xsd::TIME,
                        TypedValueRef::DateLiteral(_) => xsd::DATE,
                        TypedValueRef::DurationLiteral(_) => xsd::DURATION,
                        TypedValueRef::YearMonthDurationLiteral(_) => {
                            xsd::YEAR_MONTH_DURATION
                        }
                        TypedValueRef::DayTimeDurationLiteral(_) => {
                            xsd::DAY_TIME_DURATION
                        }
                        TypedValueRef::OtherLiteral(value) => value.datatype(),
                    };
                    Ok(TypedValueRef::NamedNode(iri))
                },
                ThinError::expected,
            )
        }))
    }
}
