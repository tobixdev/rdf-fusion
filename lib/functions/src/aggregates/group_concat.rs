use crate::builtin::factory::GraphFusionUdafFactory;
use crate::builtin::BuiltinName;
use crate::{DFResult, FunctionName};
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use graphfusion_encoding::typed_value::decoders::StringLiteralRefTermValueDecoder;
use graphfusion_encoding::typed_value::encoders::StringLiteralRefTermValueEncoder;
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_encoding::{EncodingName, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_model::{StringLiteralRef, Term, ThinError};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct GroupConcatUdafFactory {}

impl GraphFusionUdafFactory for GroupConcatUdafFactory {
    fn name(&self) -> FunctionName {
        FunctionName::Builtin(BuiltinName::GroupConcat)
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::TypedValue]
    }

    fn create_with_args(
        &self,
        constant_args: HashMap<String, Term>,
    ) -> DFResult<Arc<AggregateUDF>> {
        // TODO support separatro
        let udaf = create_udaf(
            "group_concat",
            vec![TypedValueEncoding::data_type()],
            Arc::new(TypedValueEncoding::data_type()),
            Volatility::Immutable,
            Arc::new(move |_| Ok(Box::new(SparqlGroupConcat::new(",".to_owned())))),
            Arc::new(vec![
                DataType::Boolean,
                DataType::Utf8,
                DataType::Boolean,
                DataType::Utf8,
            ]),
        );
        Ok(Arc::new(udaf))
    }
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
    pub const SEPARATOR: &'static str = "separator";

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

        let arr = TypedValueEncoding::try_new_array(values[0].clone())?;
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
                .map(graphfusion_encoding::EncodingScalar::into_scalar_value);
        }

        let value = self.value.as_deref().unwrap_or("");
        let literal = StringLiteralRef(value, self.language.as_deref());
        StringLiteralRefTermValueEncoder::encode_term(Ok(literal))
            .map(graphfusion_encoding::EncodingScalar::into_scalar_value)
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

        let mut value_exists = self.value.is_some();
        let mut value = self.value.take().unwrap_or_default();

        let old_values = states[1].as_string::<i32>();
        for old_value in old_values.iter().flatten() {
            if value_exists {
                value += self.separator.as_str();
            }
            value += old_value;
            value_exists = true;
        }
        self.value = Some(value);

        let existing_language_error = states[2].as_boolean().iter().any(|e| e == Some(true));
        if existing_language_error {
            self.language_error = true;
            self.language = None;
            return Ok(());
        }

        let old_languages = states[3].as_string::<i32>();
        for old_language in old_languages {
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
