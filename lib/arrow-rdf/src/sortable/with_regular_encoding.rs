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
use model::{Numeric, TermRef, ThinResult};
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
                let values = (0..args.number_rows).map(|i| TermRef::from_sortable_array(array, i));
                let result = into_regular_enc(values)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array_of_size(args.number_rows)?;
                let array = as_struct_array(&array);
                let values = (0..args.number_rows).map(|i| TermRef::from_sortable_array(array, i));
                let result = into_regular_enc(values)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}

fn into_regular_enc<'data>(
    terms: impl IntoIterator<Item = ThinResult<TermRef<'data>>>,
) -> DFResult<ArrayRef> {
    let terms_iter = terms.into_iter();
    let mut builder = EncRdfTermBuilder::default();

    for term in terms_iter {
        if let Ok(term) = term {
            match term {
                TermRef::NamedNode(v) => builder.append_named_node(v.as_str())?,
                TermRef::BlankNode(v) => builder.append_blank_node(v.as_str())?,
                TermRef::BooleanLiteral(v) => builder.append_boolean(v.as_bool())?,
                TermRef::NumericLiteral(v) => match v {
                    Numeric::Int(v) => builder.append_int(v)?,
                    Numeric::Integer(v) => builder.append_integer(v)?,
                    Numeric::Float(v) => builder.append_float(v)?,
                    Numeric::Double(v) => builder.append_double(v)?,
                    Numeric::Decimal(v) => builder.append_decimal(v)?,
                },
                TermRef::SimpleLiteral(v) => builder.append_string(v.value, None)?,
                TermRef::LanguageStringLiteral(v) => {
                    builder.append_string(v.value, Some(v.language))?
                }
                TermRef::DateTimeLiteral(v) => builder.append_date_time(v)?,
                TermRef::TimeLiteral(v) => builder.append_time(v)?,
                TermRef::DateLiteral(v) => builder.append_date(v)?,
                TermRef::DurationLiteral(v) => {
                    builder.append_duration(Some(v.year_month()), Some(v.day_time()))?
                }
                TermRef::YearMonthDurationLiteral(v) => builder.append_duration(Some(v), None)?,
                TermRef::DayTimeDurationLiteral(v) => builder.append_duration(None, Some(v))?,
                TermRef::TypedLiteral(v) => {
                    builder.append_typed_literal(v.value, v.literal_type)?
                }
            }
        } else {
            builder.append_null()?
        }
    }

    builder.finish()
}
