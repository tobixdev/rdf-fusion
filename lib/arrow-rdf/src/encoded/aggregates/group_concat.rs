use crate::encoded::scalars::encode_scalar_null;
use crate::encoded::write_enc_term::WriteEncTerm;
use crate::encoded::{EncTerm, FromEncodedTerm};
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use datamodel::StringLiteralRef;
use std::sync::Arc;

pub fn enc_group_concat(separator: impl Into<String>) -> AggregateUDF {
    let separator = separator.into();
    create_udaf(
        "enc_group_concat",
        vec![EncTerm::data_type()],
        Arc::new(EncTerm::data_type()),
        Volatility::Immutable,
        Arc::new(move |_| Ok(Box::new(SparqlGroupConcat::new(separator.clone())))),
        Arc::new(vec![
            DataType::Boolean,
            DataType::Utf8,
            DataType::Boolean,
            DataType::Utf8,
        ]),
    )
}

#[derive(Debug)]
struct SparqlGroupConcat {
    separator: String,
    error: bool,
    value: String,
    language_error: bool,
    language: Option<String>,
}

impl SparqlGroupConcat {
    pub fn new(separator: String) -> Self {
        SparqlGroupConcat {
            separator,
            error: false,
            value: "".to_owned(),
            language_error: false,
            language: None,
        }
    }
}

impl Accumulator for SparqlGroupConcat {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if self.error || values.is_empty() {
            return Ok(());
        }

        let arr = as_enc_term_array(&values[0]).expect("Type constraint.");

        for i in 0..arr.len() {
            let string = StringLiteralRef::from_enc_array(arr, i);
            match string {
                Ok(string) => {
                    self.value += string.0;
                    if let Some(lang) = &self.language {
                        if Some(lang.as_str()) != string.1 {
                            self.language_error = true;
                            self.language = None;
                        }
                    } else {
                        self.language = string.1.map(ToOwned::to_owned);
                    }
                }
                Err(_) => {
                    self.error = true;
                    self.value = "".to_owned();
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.error {
            return Ok(encode_scalar_null());
        }

        let literal = StringLiteralRef(
            self.value.as_str(),
            self.language.as_ref().map(|l| l.as_str()),
        );
        Ok(literal.into_scalar_value()?)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Boolean(Some(self.error)),
            ScalarValue::Utf8(Some(self.value.clone())),
            ScalarValue::Boolean(Some(self.language_error)),
            ScalarValue::Utf8(self.language.clone()),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let error = states[0].as_boolean().iter().any(|e| e == Some(true));
        if error {
            self.error = true;
            self.value = "".to_owned();
            self.language = None;
            return Ok(());
        }

        let old_values = states[1].as_string::<i32>();
        for old_value in old_values.iter().skip(1) {
            self.value += self.separator.as_str();
            self.value += old_value.unwrap();
        }

        let existing_language_error = states[2].as_boolean().iter().any(|e| e == Some(true));
        if existing_language_error {
            self.language_error = true;
            self.language = None;
            return Ok(());
        }

        let old_languages = states[3].as_string::<i32>();
        for old_language in old_languages.iter() {
            if let Some(lang) = &self.language {
                if Some(lang.as_str()) != old_language {
                    self.language_error = true;
                    self.language = None;
                }
            } else {
                self.language = old_language.map(ToOwned::to_owned);
            }
        }

        Ok(())
    }
}
