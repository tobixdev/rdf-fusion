use crate::value_encoding::scalars::{encode_scalar_null, encode_scalar_term};
use crate::value_encoding::write_enc_term::WriteEncTerm;
use crate::value_encoding::{FromEncodedTerm, RdfValueEncoding};
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use model::{InternalTerm, InternalTermRef, ThinError, ThinResult};
use std::sync::{Arc, LazyLock};

pub static ENC_MIN: LazyLock<AggregateUDF> = LazyLock::new(|| {
    create_udaf(
        "enc_min",
        vec![RdfValueEncoding::data_type()],
        Arc::new(RdfValueEncoding::data_type()),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(SparqlMin::new()))),
        Arc::new(vec![DataType::Boolean, RdfValueEncoding::data_type()]),
    )
});

#[derive(Debug)]
struct SparqlMin {
    executed_once: bool,
    min: ThinResult<InternalTerm>,
}

impl SparqlMin {
    pub fn new() -> Self {
        SparqlMin {
            executed_once: false,
            min: ThinError::expected(),
        }
    }

    fn on_new_value(&mut self, value: ThinResult<InternalTermRef<'_>>) {
        if !self.executed_once {
            self.min = value.map(InternalTermRef::into_owned);
            self.executed_once = true;
        } else if let Ok(min) = self.min.as_ref() {
            if let Ok(value) = value {
                if value < min.as_ref() {
                    self.min = Ok(value.into_owned());
                }
            }
        }
    }
}

impl Accumulator for SparqlMin {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        // If we already have an error, we can simply stop doing anything.
        if self.executed_once && self.min.is_err() {
            return Ok(());
        }

        let arr = as_enc_term_array(&values[0])?;

        for i in 0..arr.len() {
            let value = InternalTermRef::from_enc_array(arr, i);
            self.on_new_value(value);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let value = match self.min.as_ref() {
            Ok(value) => value.as_ref().into_scalar_value()?,
            Err(_) => encode_scalar_null(),
        };
        Ok(value)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let value = match self.min.as_ref().map(|v| v.as_ref()) {
            Ok(value) => encode_scalar_term(value.into_decoded().as_ref())?,
            Err(_) => encode_scalar_null(),
        };
        Ok(vec![ScalarValue::Boolean(Some(self.executed_once)), value])
    }

    #[allow(clippy::missing_asserts_for_indexing)]
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.len() != 2 {
            return exec_err!("Unexpected number of states.");
        }

        let executed_once = states[0].as_boolean();
        let terms = as_enc_term_array(&states[1])?;
        for i in 0..states[0].len() {
            if executed_once.value(i) {
                let value = InternalTermRef::from_enc_array(terms, i);
                self.on_new_value(value);
            }
        }

        Ok(())
    }
}
