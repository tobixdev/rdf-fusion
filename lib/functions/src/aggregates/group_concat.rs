use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::plan_err;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::function::{
    AccumulatorArgs, AggregateFunctionSimplification,
};
use datafusion::logical_expr::{
    AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
use rdf_fusion_encoding::typed_value::decoders::{
    DefaultTypedValueDecoder, StringLiteralRefTermValueDecoder,
};
use rdf_fusion_encoding::typed_value::encoders::StringLiteralRefTermValueEncoder;
use rdf_fusion_encoding::{TermDecoder, TermEncoder, TermEncoding};
use rdf_fusion_model::{StringLiteralRef, ThinError, TypedValueRef};
use std::any::Any;
use std::sync::Arc;

pub fn group_concat_typed_value() -> AggregateUDF {
    AggregateUDF::new_from_impl(SparqlGroupConcat::new())
}

/// Concatenates the strings in a set with a given separator.
///
/// Relevant Resources:
/// - [SPARQL 1.1 - GROUP CONCAT](https://www.w3.org/TR/sparql11-query/#defn_aggGroupConcat)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparqlGroupConcat {
    name: String,
    signature: Signature,
}

impl SparqlGroupConcat {
    /// Creates a new [SparqlGroupConcat] aggregate UDF.
    pub fn new() -> Self {
        let name = BuiltinName::GroupConcat.to_string();
        let signature = Signature::uniform(
            2,
            vec![TYPED_VALUE_ENCODING.data_type()],
            Volatility::Stable,
        );
        SparqlGroupConcat { name, signature }
    }
}

impl Default for SparqlGroupConcat {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for SparqlGroupConcat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(TYPED_VALUE_ENCODING.data_type())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        unreachable!("GROUP_CONCAT should have been simplified by the optimizer")
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        Some(Box::new(|function, _info| {
            debug_assert!(
                function.params.args.len() == 2,
                "Separator should be the second argument"
            );

            let separator_expr = &function.params.args[1];
            let separator = match separator_expr {
                Expr::Literal(value, _) => {
                    let scalar = TYPED_VALUE_ENCODING.try_new_scalar(value.clone())?;
                    let term = DefaultTypedValueDecoder::decode_term(&scalar);
                    match term {
                        Ok(TypedValueRef::SimpleLiteral(literal)) => {
                            literal.value.to_owned()
                        }
                        Err(_) => " ".to_owned(),
                        _ => return plan_err!("Separator should be a simple literal"),
                    }
                }
                _ => return plan_err!("Separator should be a literal"),
            };

            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                AggregateUDF::new_from_impl(SparqlGroupConcatWithSeparator::new(
                    separator,
                ))
                .into(),
                vec![function.params.args[0].clone()],
                function.params.distinct,
                function.params.filter.clone(),
                function.params.order_by.clone(),
                function.params.null_treatment,
            )))
        }))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparqlGroupConcatWithSeparator {
    name: String,
    signature: Signature,
    separator: String,
}

impl SparqlGroupConcatWithSeparator {
    /// Creates a new [SparqlGroupConcatWithSeparator] aggregate UDF.
    pub fn new(separator: String) -> Self {
        let name = BuiltinName::GroupConcat.to_string();
        let signature =
            Signature::exact(vec![TYPED_VALUE_ENCODING.data_type()], Volatility::Stable);
        SparqlGroupConcatWithSeparator {
            name,
            signature,
            separator,
        }
    }
}

impl AggregateUDFImpl for SparqlGroupConcatWithSeparator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(TYPED_VALUE_ENCODING.data_type())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SparqlGroupConcatAccumulator::new(
            self.separator.clone(),
        )))
    }
}

#[derive(Debug)]
struct SparqlGroupConcatAccumulator {
    separator: String,
    error: bool,
    value: Option<String>,
    language_error: bool,
    language: Option<String>,
}

impl SparqlGroupConcatAccumulator {
    pub fn new(separator: String) -> Self {
        SparqlGroupConcatAccumulator {
            separator,
            error: false,
            value: None,
            language_error: false,
            language: None,
        }
    }
}

impl Accumulator for SparqlGroupConcatAccumulator {
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
