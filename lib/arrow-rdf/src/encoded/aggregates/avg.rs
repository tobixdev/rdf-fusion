use crate::encoded::scalars::encode_scalar_null;
use crate::encoded::write_enc_term::WriteEncTerm;
use crate::encoded::{EncTerm, FromEncodedTerm};
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::logical_expr::{create_udaf, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use datamodel::{Decimal, Integer, Numeric, NumericPair, RdfOpResult};
use std::ops::Div;
use std::sync::Arc;

pub const ENC_AVG: once_cell::unsync::Lazy<datafusion::logical_expr::AggregateUDF> =
    once_cell::unsync::Lazy::new(|| {
        create_udaf(
            "enc_avg",
            vec![EncTerm::data_type()],
            Arc::new(EncTerm::data_type()),
            Volatility::Immutable,
            Arc::new(|_| Ok(Box::new(SparqlAvg::new()))),
            Arc::new(vec![EncTerm::data_type(), DataType::Int64]),
        )
    });

#[derive(Debug)]
struct SparqlAvg {
    sum: RdfOpResult<Numeric>,
    count: i64,
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
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() || self.sum.is_err() {
            return Ok(());
        }
        let arr = as_enc_term_array(&values[0]).expect("Type constraint.");

        self.count += arr.len() as i64;
        for i in 0..arr.len() {
            let value = Numeric::from_enc_array(arr, i);
            if let Ok(sum) = self.sum {
                if let Ok(value) = value {
                    self.sum = match NumericPair::with_casts_from(sum, value) {
                        NumericPair::Int(lhs, rhs) => {
                            lhs.checked_add(rhs).map(Numeric::Int).ok_or(())
                        }
                        NumericPair::Integer(lhs, rhs) => {
                            lhs.checked_add(rhs).map(Numeric::Integer).ok_or(())
                        }
                        NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs + rhs)),
                        NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs + rhs)),
                        NumericPair::Decimal(lhs, rhs) => {
                            lhs.checked_add(rhs).map(Numeric::Decimal).ok_or(())
                        }
                    };
                } else {
                    self.sum = Err(());
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.count == 0 {
            return Ok(Integer::from(self.count).into_scalar_value()?);
        }

        if self.sum.is_err() {
            return Ok(encode_scalar_null());
        }

        let count = Numeric::Decimal(Decimal::from(self.count));
        let result = match NumericPair::with_casts_from(self.sum.unwrap(), count) {
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
        Ok(vec![value, ScalarValue::Int64(Some(self.count))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let arr = as_enc_term_array(&states[0]).expect("Type constraint.");
        let counts = states[1].as_primitive::<Int64Type>();
        for i in 0..arr.len() {
            let value = Numeric::from_enc_array(arr, i);
            if let Ok(sum) = self.sum {
                if let Ok(value) = value {
                    self.sum = match NumericPair::with_casts_from(sum, value) {
                        NumericPair::Int(lhs, rhs) => {
                            lhs.checked_add(rhs).map(Numeric::Int).ok_or(())
                        }
                        NumericPair::Integer(lhs, rhs) => {
                            lhs.checked_add(rhs).map(Numeric::Integer).ok_or(())
                        }
                        NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs + rhs)),
                        NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs + rhs)),
                        NumericPair::Decimal(lhs, rhs) => {
                            lhs.checked_add(rhs).map(Numeric::Decimal).ok_or(())
                        }
                    };
                } else {
                    self.sum = Err(());
                }
            }
            self.count += counts.value(i);
        }

        Ok(())
    }
}
