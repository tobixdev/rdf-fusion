use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{AggregateUDF, Volatility, create_udaf};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
use rdf_fusion_encoding::typed_value::decoders::StringLiteralRefTermValueDecoder;
use rdf_fusion_encoding::typed_value::encoders::StringLiteralRefTermValueEncoder;
use rdf_fusion_encoding::{TermDecoder, TermEncoder, TermEncoding};
use rdf_fusion_model::{StringLiteralRef, ThinError};
use std::sync::Arc;

pub fn group_concat_typed_value(separator: Option<String>) -> Arc<AggregateUDF> {
    let separator = separator.unwrap_or_else(|| " ".to_owned());
    let udaf = create_udaf(
        "group_concat",
        vec![TYPED_VALUE_ENCODING.data_type()],
        Arc::new(TYPED_VALUE_ENCODING.data_type()),
        Volatility::Immutable,
        Arc::new(move |_| Ok(Box::new(SparqlGroupConcat::new(separator.clone())))),
        Arc::new(vec![
            DataType::Boolean,
            DataType::Utf8,
            DataType::Boolean,
            DataType::Utf8,
        ]),
    );
    Arc::new(udaf)
}

#[derive(Debug)]
struct SparqlGroupConcat {
    separator: String,
    error: bool,
    value: Option<String>,
    language_error: bool,
    language: Option<String>,
}

impl SparqlGroupConcat {
    pub fn new(separator: String) -> Self {
        SparqlGroupConcat {
            separator,
            error: false,
            value: None,
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

        let mut value_exists = self.value.is_some();
        let mut value = self.value.take().unwrap_or_default();

        let arr = TYPED_VALUE_ENCODING.try_new_array(Arc::clone(&values[0]))?;
        for string in StringLiteralRefTermValueDecoder::decode_terms(&arr) {
            if let Ok(string) = string {
                if value_exists {
                    value += self.separator.as_str();
                }
                value += string.0;
                value_exists = true;
                if let Some(lang) = &self.language {
                    if Some(lang.as_str()) != string.1 {
                        self.language_error = true;
                        self.language = None;
                    }
                } else {
                    self.language = string.1.map(ToOwned::to_owned);
                }
            } else {
                self.error = true;
                self.value = None;
                return Ok(());
            }
        }

        self.value = Some(value);
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.error {
            return StringLiteralRefTermValueEncoder::encode_term(ThinError::expected())
                .map(rdf_fusion_encoding::EncodingScalar::into_scalar_value);
        }

        let value = self.value.as_deref().unwrap_or("");
        let literal = StringLiteralRef(value, self.language.as_deref());
        StringLiteralRefTermValueEncoder::encode_term(Ok(literal))
            .map(rdf_fusion_encoding::EncodingScalar::into_scalar_value)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Boolean(Some(self.error)),
            ScalarValue::Utf8(self.value.clone()),
            ScalarValue::Boolean(Some(self.language_error)),
            ScalarValue::Utf8(self.language.clone()),
        ])
    }

    #[allow(clippy::missing_asserts_for_indexing)]
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let error = states[0].as_boolean().iter().any(|e| e == Some(true));
        if error {
            self.error = true;
            self.value = None;
            self.language = None;
            return Ok(());
        }

        let old_values = states[1].as_string::<i32>();
        for old_value in old_values.iter().flatten() {
            self.value = match self.value.take() {
                None => Some(old_value.to_owned()),
                Some(value) => Some(value + self.separator.as_str() + old_value),
            };
        }

        let existing_language_error =
            states[2].as_boolean().iter().any(|e| e == Some(true));
        if existing_language_error {
            self.language_error = true;
            self.language = None;
            return Ok(());
        }

        let old_languages = states[3].as_string::<i32>();
        for old_language in old_languages {
            self.language = match (self.language.take(), old_language) {
                (None, other) => other.map(ToOwned::to_owned),
                (other, None) => other,
                (Some(language), Some(old_language)) => {
                    if language.as_str() == old_language {
                        Some(language)
                    } else {
                        self.language_error = true;
                        None
                    }
                }
            };
        }

        Ok(())
    }
}
