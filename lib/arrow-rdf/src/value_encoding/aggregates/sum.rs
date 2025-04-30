use crate::value_encoding::scalars::encode_scalar_null;
use crate::value_encoding::write_enc_term::WriteEncTerm;
use crate::value_encoding::{FromEncodedTerm, RdfValueEncoding};
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use model::{Integer, Numeric, NumericPair, ThinResult};
use std::sync::{Arc, LazyLock};

pub static ENC_SUM: LazyLock<AggregateUDF> = LazyLock::new(|| {
    create_udaf(
        "enc_sum",
        vec![RdfValueEncoding::data_type()],
        Arc::new(RdfValueEncoding::data_type()),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(SparqlSum::new()))),
        Arc::new(vec![RdfValueEncoding::data_type()]),
    )
});

#[derive(Debug)]
struct SparqlSum {
    sum: ThinResult<Numeric>,
}

impl SparqlSum {
    pub fn new() -> Self {
        SparqlSum {
            sum: Ok(Numeric::Integer(Integer::from(0))),
        }
    }
}

impl Accumulator for SparqlSum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = as_enc_term_array(&values[0])?;

        // TODO: Can we stop once we error?

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
                }
            }
        }

        Ok(())
    }

    // DataFusion expects this function to return the final value of this aggregator.
    // in this case, this is the formula of the geometric mean
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let value = match self.sum {
            Ok(value) => value.into_scalar_value()?,
            Err(_) => encode_scalar_null(),
        };
        Ok(value)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let value = match self.sum {
            Ok(value) => value.into_scalar_value()?,
            Err(_) => encode_scalar_null(),
        };
        Ok(vec![value])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}
