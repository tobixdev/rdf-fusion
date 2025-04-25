use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::sortable::from_sortable_term::FromSortableTerm;
use crate::sortable::SortableTerm;
use crate::DFResult;
use datafusion::arrow::array::{as_struct_array, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use model::{Numeric, InternalTermRef, ThinResult};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncWithRegularEncoding {
    signature: Signature,
}

impl EncWithRegularEncoding {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![SortableTerm::data_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EncWithRegularEncoding {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_with_regular_encoding"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(EncTerm::data_type())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs<'_>,
    ) -> datafusion::common::Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("Unexpected number of arguments");
        }

        match &args.args[0] {
            ColumnarValue::Array(array) => {
                let array = as_struct_array(array);
                let values = (0..args.number_rows).map(|i| InternalTermRef::from_sortable_array(array, i));
                let result = into_regular_enc(values)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array_of_size(args.number_rows)?;
                let array = as_struct_array(&array);
                let values = (0..args.number_rows).map(|i| InternalTermRef::from_sortable_array(array, i));
                let result = into_regular_enc(values)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}

fn into_regular_enc<'data>(
    terms: impl IntoIterator<Item = ThinResult<InternalTermRef<'data>>>,
) -> DFResult<ArrayRef> {
    let terms_iter = terms.into_iter();
    let mut builder = EncRdfTermBuilder::default();

    for term in terms_iter {
        if let Ok(term) = term {
            match term {
                InternalTermRef::NamedNode(v) => builder.append_named_node(v.as_str())?,
                InternalTermRef::BlankNode(v) => builder.append_blank_node(v.as_str())?,
                InternalTermRef::BooleanLiteral(v) => builder.append_boolean(v.as_bool())?,
                InternalTermRef::NumericLiteral(v) => match v {
                    Numeric::Int(v) => builder.append_int(v)?,
                    Numeric::Integer(v) => builder.append_integer(v)?,
                    Numeric::Float(v) => builder.append_float(v)?,
                    Numeric::Double(v) => builder.append_double(v)?,
                    Numeric::Decimal(v) => builder.append_decimal(v)?,
                },
                InternalTermRef::SimpleLiteral(v) => builder.append_string(v.value, None)?,
                InternalTermRef::LanguageStringLiteral(v) => {
                    builder.append_string(v.value, Some(v.language))?
                }
                InternalTermRef::DateTimeLiteral(v) => builder.append_date_time(v)?,
                InternalTermRef::TimeLiteral(v) => builder.append_time(v)?,
                InternalTermRef::DateLiteral(v) => builder.append_date(v)?,
                InternalTermRef::DurationLiteral(v) => {
                    builder.append_duration(Some(v.year_month()), Some(v.day_time()))?
                }
                InternalTermRef::YearMonthDurationLiteral(v) => builder.append_duration(Some(v), None)?,
                InternalTermRef::DayTimeDurationLiteral(v) => builder.append_duration(None, Some(v))?,
                InternalTermRef::TypedLiteral(v) => {
                    builder.append_typed_literal(v.value, v.literal_type)?
                }
            }
        } else {
            builder.append_null()?
        }
    }

    builder.finish()
}
