use crate::encoded::scalars::encode_scalar_null;
use crate::encoded::write_enc_term::WriteEncTerm;
use crate::encoded::{EncTerm, FromEncodedTerm};
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, UInt64Type};
use datafusion::common::exec_datafusion_err;
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use model::{Decimal, Integer, Numeric, NumericPair, ThinError, ThinResult};
use std::ops::Div;
use std::sync::{Arc, LazyLock};

pub static ENC_AVG: LazyLock<AggregateUDF> = LazyLock::new(|| {
    create_udaf(
        "enc_avg",
        vec![EncTerm::data_type()],
        Arc::new(EncTerm::data_type()),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(SparqlAvg::new()))),
        Arc::new(vec![EncTerm::data_type(), DataType::UInt64]),
    )
});

#[derive(Debug)]
struct SparqlAvg {
    sum: ThinResult<Numeric>,
    count: u64,
}

impl SparqlAvg {
    pub fn new() -> Self {
        SparqlAvg {
            sum: Ok(Numeric::Decimal(Decimal::from(0))),
            count: 0,
        }
    }
}

impl Accumulator for SparqlAvg {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.is_empty() || self.sum.is_err() {
            return Ok(());
        }
        let arr = as_enc_term_array(&values[0])?;
        let arr_len =
            u64::try_from(arr.len()).map_err(|_| exec_datafusion_err!("Array was too large."))?;
        self.count += arr_len;

        for i in 0..arr.len() {
            let value = Numeric::from_enc_array(arr, i);
            if let Ok(sum) = self.sum {
                if let Ok(value) = value {
                    self.sum = match NumericPair::with_casts_from(sum, value) {
                        NumericPair::Int(lhs, rhs) => lhs.checked_add(rhs).map(Numeric::Int),
                        NumericPair::Integer(lhs, rhs) => {
                            lhs.checked_add(rhs).map(Numeric::Integer)
                        }
                        NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs + rhs)),
                        NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs + rhs)),
                        NumericPair::Decimal(lhs, rhs) => {
                            lhs.checked_add(rhs).map(Numeric::Decimal)
                        }
                    };
                } else {
                    self.sum = ThinError::expected();
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.count == 0 {
            let count = i64::try_from(self.count)
                .map_err(|_| exec_datafusion_err!("Count too large for current xsd::Integer"))?;
            return Integer::from(count).into_scalar_value();
        }

        let Ok(sum) = self.sum else {
            return Ok(encode_scalar_null());
        };

        let count = Numeric::Decimal(Decimal::from(self.count));
        let result = match NumericPair::with_casts_from(sum, count) {
            NumericPair::Int(_, _) => unreachable!("Starts with Integer"),
            NumericPair::Integer(lhs, rhs) => lhs
                .checked_div(rhs)
                .map(WriteEncTerm::into_scalar_value)
                .unwrap_or(Ok(encode_scalar_null())),
            NumericPair::Float(lhs, rhs) => lhs.div(rhs).into_scalar_value(),
            NumericPair::Double(lhs, rhs) => lhs.div(rhs).into_scalar_value(),
            NumericPair::Decimal(lhs, rhs) => lhs
                .checked_div(rhs)
                .map(WriteEncTerm::into_scalar_value)
                .unwrap_or(Ok(encode_scalar_null())),
        }?;
        Ok(result)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let value = match self.sum {
            Ok(value) => value.into_scalar_value()?,
            Err(_) => encode_scalar_null(),
        };
        Ok(vec![value, ScalarValue::UInt64(Some(self.count))])
    }

    #[allow(clippy::missing_asserts_for_indexing)]
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let arr = as_enc_term_array(&states[0])?;
        let counts = states[1].as_primitive::<UInt64Type>();
        for i in 0..arr.len() {
            let value = Numeric::from_enc_array(arr, i);
            if let Ok(sum) = self.sum {
                if let Ok(value) = value {
                    self.sum = match NumericPair::with_casts_from(sum, value) {
                        NumericPair::Int(lhs, rhs) => lhs.checked_add(rhs).map(Numeric::Int),
                        NumericPair::Integer(lhs, rhs) => {
                            lhs.checked_add(rhs).map(Numeric::Integer)
                        }
                        NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs + rhs)),
                        NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs + rhs)),
                        NumericPair::Decimal(lhs, rhs) => {
                            lhs.checked_add(rhs).map(Numeric::Decimal)
                        }
                    };
                } else {
                    self.sum = ThinError::expected();
                }
            }
            self.count += counts.value(i);
        }

        Ok(())
    }
}
