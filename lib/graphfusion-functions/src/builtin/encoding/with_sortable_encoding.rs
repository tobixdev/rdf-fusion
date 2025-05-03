use crate::sortable_encoding::{SortableTerm, SortableTermBuilder};
use crate::FromArrow;
use crate::value_encoding::RdfTermValueEncoding;
use crate::DFResult;
use datafusion::arrow::array::{as_union_array, StructArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use model::{RdfTermValueRef, ThinResult};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncWithSortableEncoding {
    signature: Signature,
}

impl Default for EncWithSortableEncoding {
    fn default() -> Self {
        Self::new()
    }
}

impl EncWithSortableEncoding {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![RdfTermValueEncoding::datatype()]),
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
                let values = (0..args.number_rows).map(|i| RdfTermValueRef::from_array(array, i));
                let result = into_struct_enc(values);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(scalar) => {
                let term = RdfTermValueRef::from_scalar(scalar);
                let result = into_struct_enc([term]);
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            }
        }
    }
}

fn into_struct_enc<'data>(
    terms: impl IntoIterator<Item = ThinResult<RdfTermValueRef<'data>>>,
) -> StructArray {
    let terms_iter = terms.into_iter();

    let (_, size_upper_bound) = terms_iter.size_hint();
    let mut builder = SortableTermBuilder::new(size_upper_bound.unwrap_or(0));

    for term in terms_iter {
        if let Ok(term) = term {
            match term {
                RdfTermValueRef::NamedNode(v) => builder.append_named_node(v),
                RdfTermValueRef::BlankNode(v) => builder.append_blank_node(v),
                RdfTermValueRef::BooleanLiteral(v) => builder.append_boolean(v),
                RdfTermValueRef::NumericLiteral(v) => {
                    builder.append_numeric(v, v.to_be_bytes().as_ref())
                }
                RdfTermValueRef::SimpleLiteral(v) => builder.append_string(v.value),
                RdfTermValueRef::LanguageStringLiteral(v) => builder.append_string(v.value),
                RdfTermValueRef::DateTimeLiteral(v) => builder.append_date_time(v),
                RdfTermValueRef::TimeLiteral(v) => builder.append_time(v),
                RdfTermValueRef::DateLiteral(v) => builder.append_date(v),
                RdfTermValueRef::DurationLiteral(v) => builder.append_duration(v),
                RdfTermValueRef::YearMonthDurationLiteral(v) => {
                    builder.append_year_month_duration(v)
                }
                RdfTermValueRef::DayTimeDurationLiteral(v) => builder.append_day_time_duration(v),
                RdfTermValueRef::OtherLiteral(v) => builder.append_literal(v),
            }
        } else {
            builder.append_null()
        }
    }

    builder.finish()
}
