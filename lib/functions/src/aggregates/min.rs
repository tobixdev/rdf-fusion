use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::typed_value::TypedValueEncodingRef;
use rdf_fusion_encoding::{EncodingScalar, TermDecoder, TermEncoder, TermEncoding};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{ThinError, ThinResult, TypedValue, TypedValueRef};
use std::sync::Arc;

pub fn min_typed_value(encoding: TypedValueEncodingRef) -> AggregateUDF {
    let data_type = encoding.data_type().clone();
    create_udaf(
        "MIN",
        vec![data_type.clone()],
        Arc::new(data_type.clone()),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(SparqlTypedValueMin::new(encoding)))),
        Arc::new(vec![DataType::Boolean, data_type.clone()]),
    )
}

#[derive(Debug)]
struct SparqlTypedValueMin {
    encoding: TypedValueEncodingRef,
    executed_once: bool,
    min: ThinResult<TypedValue>,
}

impl SparqlTypedValueMin {
    pub fn new(encoding: TypedValueEncodingRef) -> Self {
        SparqlTypedValueMin {
            encoding,
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

impl Accumulator for SparqlTypedValueMin {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        // If we already have an error, we can simply stop doing anything.
        if self.executed_once && self.min.is_err() {
            return Ok(());
        }

        let arr = self.encoding.try_new_array(Arc::clone(&values[0]))?;

        for value in DefaultTypedValueDecoder::decode_terms(&arr) {
            self.on_new_value(value);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let value = match self.min.as_ref() {
            Ok(value) => self
                .encoding
                .default_encoder()
                .encode_term(Ok(value.as_ref()))?,
            Err(_) => self
                .encoding
                .default_encoder()
                .encode_term(ThinError::expected())?,
        };
        Ok(value.into_scalar_value())
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let value = match self.min.as_ref().map(|v| v.as_ref()) {
            Ok(value) => self.encoding.default_encoder().encode_term(Ok(value))?,
            Err(_) => self
                .encoding
                .default_encoder()
                .encode_term(ThinError::expected())?,
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

        let array = self.encoding.try_new_array(Arc::clone(&states[1]))?;
        let terms = DefaultTypedValueDecoder::decode_terms(&array);
        for (is_valid, term) in executed_once.iter().zip(terms) {
            if is_valid == Some(true) {
                self.on_new_value(term);
            }
        }

        Ok(())
    }
}
