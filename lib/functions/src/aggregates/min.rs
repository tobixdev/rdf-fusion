use std::collections::HashMap;
use crate::{DFResult, FunctionName};
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use graphfusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use graphfusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_encoding::{EncodingName, EncodingScalar, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_model::{Term, ThinError, ThinResult, TypedValue, TypedValueRef};
use std::sync::{Arc, LazyLock};
use crate::aggregates::ENC_AVG;
use crate::builtin::BuiltinName;
use crate::factory::GraphFusionUdafFactory;

pub static TYPED_VALUE_MIN: LazyLock<AggregateUDF> = LazyLock::new(|| {
    create_udaf(
        "enc_min",
        vec![TypedValueEncoding::data_type()],
        Arc::new(TypedValueEncoding::data_type()),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(SparqlMin::new()))),
        Arc::new(vec![DataType::Boolean, TypedValueEncoding::data_type()]),
    )
});

#[derive(Debug)]
pub struct MinUdafFactory {}

impl GraphFusionUdafFactory for MinUdafFactory {
    fn name(&self) -> FunctionName {
        FunctionName::Builtin(BuiltinName::Avg)
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::TypedValue]
    }

    fn create_with_args(
        &self,
        constant_args: HashMap<String, Term>,
    ) -> DFResult<Arc<AggregateUDF>> {
        Ok(Arc::clone(&ENC_AVG))
    }
}

#[derive(Debug)]
struct SparqlMin {
    executed_once: bool,
    min: ThinResult<TypedValue>,
}

impl SparqlMin {
    pub fn new() -> Self {
        SparqlMin {
            executed_once: false,
            min: ThinError::expected(),
        }
    }

    fn on_new_value(&mut self, value: ThinResult<TypedValueRef<'_>>) {
        if !self.executed_once {
            self.min = value.map(TypedValueRef::into_owned);
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

        let arr = TypedValueEncoding::try_new_array(Arc::clone(&values[0]))?;

        for value in DefaultTypedValueDecoder::decode_terms(&arr) {
            self.on_new_value(value);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let value = match self.min.as_ref() {
            Ok(value) => DefaultTypedValueEncoder::encode_term(Ok(value.as_ref()))?,
            Err(_) => DefaultTypedValueEncoder::encode_term(ThinError::expected())?,
        };
        Ok(value.into_scalar_value())
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let value = match self.min.as_ref().map(|v| v.as_ref()) {
            Ok(value) => DefaultTypedValueEncoder::encode_term(Ok(value))?,
            Err(_) => DefaultTypedValueEncoder::encode_term(ThinError::expected())?,
        };
        Ok(vec![
            ScalarValue::Boolean(Some(self.executed_once)),
            value.into_scalar_value(),
        ])
    }

    #[allow(clippy::missing_asserts_for_indexing)]
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.len() != 2 {
            return exec_err!("Unexpected number of states.");
        }

        let executed_once = states[0].as_boolean();

        let array = TypedValueEncoding::try_new_array(Arc::clone(&states[1]))?;
        let terms = DefaultTypedValueDecoder::decode_terms(&array);
        for (is_valid, term) in executed_once.iter().zip(terms) {
            if is_valid == Some(true) {
                self.on_new_value(term);
            }
        }

        Ok(())
    }
}
