use crate::encoding::TermEncoder;
use crate::typed_value::{TypedValueArrayBuilder, TypedValueEncoding};
use crate::{DFResult, TermEncoding};
use datafusion::common::exec_err;
use rdf_fusion_model::{Numeric, ThinError, ThinResult, TypedValueRef};

#[derive(Debug)]
pub struct DefaultTypedValueEncoder;

impl TermEncoder<TypedValueEncoding> for DefaultTypedValueEncoder {
    type Term<'data> = TypedValueRef<'data>;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<TypedValueEncoding as TermEncoding>::Array> {
        let mut value_builder = TypedValueArrayBuilder::default();
        for value in terms {
            match value {
                Ok(TypedValueRef::NamedNode(value)) => value_builder.append_named_node(value)?,
                Ok(TypedValueRef::BlankNode(value)) => value_builder.append_blank_node(value)?,
                Ok(TypedValueRef::BooleanLiteral(value)) => value_builder.append_boolean(value)?,
                Ok(TypedValueRef::NumericLiteral(Numeric::Float(value))) => {
                    value_builder.append_float(value)?
                }
                Ok(TypedValueRef::NumericLiteral(Numeric::Double(value))) => {
                    value_builder.append_double(value)?
                }
                Ok(TypedValueRef::NumericLiteral(Numeric::Decimal(value))) => {
                    value_builder.append_decimal(value)?
                }
                Ok(TypedValueRef::NumericLiteral(Numeric::Int(value))) => {
                    value_builder.append_int(value)?
                }
                Ok(TypedValueRef::NumericLiteral(Numeric::Integer(value))) => {
                    value_builder.append_integer(value)?
                }
                Ok(TypedValueRef::SimpleLiteral(value)) => {
                    value_builder.append_string(value.value, None)?
                }
                Ok(TypedValueRef::LanguageStringLiteral(value)) => {
                    value_builder.append_string(value.value, Some(value.language))?
                }
                Ok(TypedValueRef::DateTimeLiteral(value)) => {
                    value_builder.append_date_time(value)?
                }
                Ok(TypedValueRef::TimeLiteral(value)) => value_builder.append_time(value)?,
                Ok(TypedValueRef::DateLiteral(value)) => value_builder.append_date(value)?,
                Ok(TypedValueRef::DurationLiteral(value)) => value_builder
                    .append_duration(Some(value.year_month()), Some(value.day_time()))?,
                Ok(TypedValueRef::YearMonthDurationLiteral(value)) => {
                    value_builder.append_duration(Some(value), None)?
                }
                Ok(TypedValueRef::DayTimeDurationLiteral(value)) => {
                    value_builder.append_duration(None, Some(value))?
                }
                Ok(TypedValueRef::OtherLiteral(value)) => {
                    value_builder.append_other_literal(value)?
                }
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        TypedValueEncoding::try_new_array(value_builder.finish())
    }
}
