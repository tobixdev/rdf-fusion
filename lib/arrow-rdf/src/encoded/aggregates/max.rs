use crate::encoded::scalars::{encode_scalar_null, encode_scalar_term};
use crate::encoded::write_enc_term::WriteEncTerm;
use crate::encoded::{EncTerm, FromEncodedTerm};
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use model::{InternalTerm, InternalTermRef, ThinError, ThinResult};
use std::sync::{Arc, LazyLock};

pub static ENC_MAX: LazyLock<AggregateUDF> = LazyLock::new(|| {
    create_udaf(
        "enc_max",
        vec![EncTerm::data_type()],
        Arc::new(EncTerm::data_type()),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(SparqlMax::new()))),
        Arc::new(vec![EncTerm::data_type()]),
    )
});

#[derive(Debug)]
struct SparqlMax {
    max: ThinResult<InternalTerm>,
    executed_once: bool,
}

impl SparqlMax {
    pub fn new() -> Self {
        SparqlMax {
            max: ThinError::expected(),
            executed_once: false,
        }
    }
}

impl Accumulator for SparqlMax {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        // TODO: Can we stop once we error?

        let arr = as_enc_term_array(&values[0])?;

        for i in 0..arr.len() {
            let value = InternalTermRef::from_enc_array(arr, i);

            if !self.executed_once {
                self.max = value.map(InternalTermRef::into_owned);
                self.executed_once = true;
            } else if let Ok(min) = self.max.as_ref() {
                if let Ok(value) = value {
                    if min.as_ref() < value {
                        self.max = Ok(value.into_owned());
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let value = match self.max.as_ref() {
            Ok(value) => value.as_ref().into_scalar_value()?,
            Err(_) => encode_scalar_null(),
        };
        Ok(value)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let value = match self.max.as_ref().map(|v| v.as_ref()) {
            Ok(value) => encode_scalar_term(value.into_decoded().as_ref())?,
            Err(_) => encode_scalar_null(),
        };
        Ok(vec![value])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}
