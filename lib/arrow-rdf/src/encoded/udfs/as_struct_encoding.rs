use crate::encoded::{EncTerm, FromEncodedTerm};
use crate::struct_encoded::{StructEncTerm, StructEncTermBuilder};
use crate::DFResult;
use datafusion::arrow::array::{as_union_array, StructArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datamodel::{RdfOpResult, TermRef};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncWithStructEncoding {
    signature: Signature,
}

impl EncWithStructEncoding {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EncWithStructEncoding {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_with_struct_encoding"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(StructEncTerm::data_type())
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!("Unexpected number of arguments");
        }

        match &args[0] {
            ColumnarValue::Array(array) => {
                let array = as_union_array(array);
                let values = (0..number_rows)
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
    let mut builder = StructEncTermBuilder::new(size_upper_bound.unwrap_or(0));

    for term in terms_iter {
        if let Ok(term) = term {
            match term {
                TermRef::NamedNode(v) => builder.append_named_node(v),
                TermRef::BlankNode(v) => builder.append_blank_node(v),
                TermRef::BooleanLiteral(v) => builder.append_boolean(v),
                TermRef::NumericLiteral(v) => builder.append_numeric(v.into()),
                TermRef::SimpleLiteral(v) => builder.append_string(v.value),
                TermRef::LanguageStringLiteral(v) => builder.append_string(v.value),
                TermRef::DateTimeLiteral(v) => builder.append_date_time(v),
                TermRef::TimeLiteral(v) => builder.append_time(v),
                TermRef::DateLiteral(v) => builder.append_date(v),
                TermRef::DurationLiteral(v) => builder.append_duration(v),
                TermRef::YearMonthDurationLiteral(v) => builder.append_year_month_duration(v),
                TermRef::DayTimeDurationLiteral(v) => builder.append_day_time_duration(v),
                TermRef::TypedLiteral(v) => builder.append_literal(v.value),
            }
        } else {
            builder.append_null()
        }
    }

    Ok(builder.finish())
}
