use crate::encoded::{EncTerm, FromEncodedTerm};
use crate::sortable::{SortableTerm, SortableTermBuilder};
use crate::DFResult;
use datafusion::arrow::array::{as_union_array, StructArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use datamodel::{RdfOpResult, TermRef};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncWithSortableEncoding {
    signature: Signature,
}

impl EncWithSortableEncoding {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::data_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EncWithSortableEncoding {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_with_sortable_encoding"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(SortableTerm::data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("Unexpected number of arguments");
        }

        match &args.args[0] {
            ColumnarValue::Array(array) => {
                let array = as_union_array(array);
                let values = (0..args.number_rows)
                    .into_iter()
                    .map(|i| TermRef::from_enc_array(array, i));
                let result = into_struct_enc(values)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(scalar) => {
                let term = TermRef::from_enc_scalar(scalar);
                let result = into_struct_enc([term])?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            }
        }
    }
}

fn into_struct_enc<'data>(
    terms: impl IntoIterator<Item = RdfOpResult<TermRef<'data>>>,
) -> DFResult<StructArray> {
    let terms_iter = terms.into_iter();

    let (_, size_upper_bound) = terms_iter.size_hint();
    let mut builder = SortableTermBuilder::new(size_upper_bound.unwrap_or(0));

    for term in terms_iter {
        if let Ok(term) = term {
            match term {
                TermRef::NamedNode(v) => builder.append_named_node(v),
                TermRef::BlankNode(v) => builder.append_blank_node(v),
                TermRef::BooleanLiteral(v) => builder.append_boolean(v),
                TermRef::NumericLiteral(v) => {
                    builder.append_numeric(v.into(), v.to_be_bytes().as_ref())
                }
                TermRef::SimpleLiteral(v) => builder.append_string(v.value, None),
                TermRef::LanguageStringLiteral(v) => {
                    builder.append_string(v.value, Some(v.language))
                }
                TermRef::DateTimeLiteral(v) => builder.append_date_time(v),
                TermRef::TimeLiteral(v) => builder.append_time(v),
                TermRef::DateLiteral(v) => builder.append_date(v),
                TermRef::DurationLiteral(v) => builder.append_duration(v),
                TermRef::YearMonthDurationLiteral(v) => builder.append_year_month_duration(v),
                TermRef::DayTimeDurationLiteral(v) => builder.append_day_time_duration(v),
                TermRef::TypedLiteral(v) => builder.append_literal(v.value, v.literal_type),
            }
        } else {
            builder.append_null()
        }
    }

    Ok(builder.finish())
}

#[cfg(test)]
mod tests {
    use crate::encoded::{EncRdfTermBuilder, FromEncodedTerm};
    use crate::sortable::FromSortableTerm;
    use crate::{as_enc_term_array, DFResult};
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl};
    use datamodel::{Date, DayTimeDuration, TermRef, Timestamp, YearMonthDuration};
    use oxrdf::vocab::xsd;
    use std::sync::Arc;

    #[test]
    fn test_with_struct_encoding_results_in_same_terms() -> DFResult<()> {
        let mut test_data_builder = EncRdfTermBuilder::new();
        test_data_builder.append_named_node("http://www.example.org/instance#a")?;
        test_data_builder.append_blank_node("blank1")?;
        test_data_builder.append_boolean(true)?;
        test_data_builder.append_int(1.into())?;
        test_data_builder.append_integer(2.into())?;
        test_data_builder.append_float(3u16.into())?;
        test_data_builder.append_double(4.into())?;
        test_data_builder.append_decimal(5.into())?;
        test_data_builder.append_date(Date::new(Timestamp::new(0.into(), None)))?;
        test_data_builder.append_duration(Some(YearMonthDuration::new(12).into()), None)?;
        test_data_builder.append_duration(None, Some(DayTimeDuration::new(30)))?;
        test_data_builder.append_duration(
            Some(YearMonthDuration::new(12).into()),
            Some(DayTimeDuration::new(30)),
        )?;
        test_data_builder.append_string("simple string", None)?;
        test_data_builder.append_string("language string", Some("en"))?;
        test_data_builder.append_typed_literal("10", xsd::SHORT.as_str())?;
        test_data_builder.append_null()?;
        let test_array = test_data_builder.finish()?;

        let number_of_rows = test_array.len();
        let udf = super::EncWithSortableEncoding::new();
        let result = udf
            .invoke_batch(
                &[ColumnarValue::Array(Arc::new(test_array.clone()))],
                number_of_rows,
            )?
            .to_array(number_of_rows)?;

        let expected_array = as_enc_term_array(&test_array)?;
        let result = result.as_struct();
        for i in 0..number_of_rows {
            let expected = TermRef::from_enc_array(expected_array, i);
            let actual = TermRef::from_sortable_array(result, i);
            assert_eq!(expected, actual);
        }

        Ok(())
    }
}
