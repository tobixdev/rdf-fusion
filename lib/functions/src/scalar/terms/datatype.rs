use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::vocab::{rdf, xsd};
use rdf_fusion_model::{Numeric, ThinError, TypedValueRef};

#[derive(Debug)]
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
    type Args<TEncoding: TermEncoding> = UnaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn return_type(&self, input_encoding: Option<EncodingName>) -> DFResult<DataType> {
        if !matches!(input_encoding, Some(EncodingName::TypedValue)) {
            return exec_err!("Unexpected target encoding: {:?}", input_encoding);
        }
        Ok(TypedValueEncoding::data_type())
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn Fn(Self::Args<TypedValueEncoding>) -> DFResult<ColumnarValue>>> {
        Some(Box::new(|UnaryArgs(arg)| {
            dispatch_unary_typed_value(
                &arg,
                |value| {
                    let iri = match value {
                        TypedValueRef::BlankNode(_) | TypedValueRef::NamedNode(_) => {
                            return ThinError::expected()
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
                        TypedValueRef::YearMonthDurationLiteral(_) => xsd::YEAR_MONTH_DURATION,
                        TypedValueRef::DayTimeDurationLiteral(_) => xsd::DAY_TIME_DURATION,
                        TypedValueRef::OtherLiteral(value) => value.datatype(),
                    };
                    Ok(TypedValueRef::NamedNode(iri))
                },
                ThinError::expected,
            )
        }))
    }
}
