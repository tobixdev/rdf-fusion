use crate::encoding::TermEncoder;
use crate::value_encoding::{TermValueEncoding, ValueArrayBuilder};
use crate::{DFResult, TermEncoding};
use datafusion::common::exec_err;
use model::{Numeric, TermValueRef, ThinError, ThinResult};

pub struct DefaultTermValueEncoder;

impl TermEncoder<TermValueEncoding> for DefaultTermValueEncoder {
    type Term<'data> = TermValueRef<'data>;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<TermValueEncoding as TermEncoding>::Array> {
        let mut value_builder = ValueArrayBuilder::default();
        for value in terms {
            match value {
                Ok(TermValueRef::NamedNode(value)) => value_builder.append_named_node(value)?,
                Ok(TermValueRef::BlankNode(value)) => value_builder.append_blank_node(value)?,
                Ok(TermValueRef::BooleanLiteral(value)) => {
                    value_builder.append_boolean(value.as_bool())?
                }
                Ok(TermValueRef::NumericLiteral(Numeric::Float(value))) => {
                    value_builder.append_float(value)?
                }
                Ok(TermValueRef::NumericLiteral(Numeric::Double(value))) => {
                    value_builder.append_double(value)?
                }
                Ok(TermValueRef::NumericLiteral(Numeric::Decimal(value))) => {
                    value_builder.append_decimal(value)?
                }
                Ok(TermValueRef::NumericLiteral(Numeric::Int(value))) => {
                    value_builder.append_int(value)?
                }
                Ok(TermValueRef::NumericLiteral(Numeric::Integer(value))) => {
                    value_builder.append_integer(value)?
                }
                Ok(TermValueRef::SimpleLiteral(value)) => {
                    value_builder.append_string(value.value, None)?
                }
                Ok(TermValueRef::LanguageStringLiteral(value)) => {
                    value_builder.append_string(value.value, Some(value.language))?
                }
                Ok(TermValueRef::DateTimeLiteral(value)) => {
                    value_builder.append_date_time(value)?
                }
                Ok(TermValueRef::TimeLiteral(value)) => value_builder.append_time(value)?,
                Ok(TermValueRef::DateLiteral(value)) => value_builder.append_date(value)?,
                Ok(TermValueRef::DurationLiteral(value)) => value_builder
                    .append_duration(Some(value.year_month()), Some(value.day_time()))?,
                Ok(TermValueRef::YearMonthDurationLiteral(value)) => {
                    value_builder.append_duration(Some(value), None)?
                }
                Ok(TermValueRef::DayTimeDurationLiteral(value)) => {
                    value_builder.append_duration(None, Some(value))?
                }
                Ok(TermValueRef::OtherLiteral(value)) => {
                    value_builder.append_typed_literal(value)?
                }
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        TermValueEncoding::try_new_array(value_builder.finish())
    }

    fn encode_term(
        term: ThinResult<TermValueRef<'_>>,
    ) -> DFResult<<TermValueEncoding as TermEncoding>::Scalar> {
        todo!()
    }
}
