use crate::builtin::BuiltinName;
use crate::DFResult;
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, UInt64Type};
use datafusion::common::exec_datafusion_err;
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use rdf_fusion_encoding::typed_value::decoders::NumericTermValueDecoder;
use rdf_fusion_encoding::typed_value::encoders::{
    DecimalTermValueEncoder, DoubleTermValueEncoder, FloatTermValueEncoder,
    IntegerTermValueEncoder, NumericTypedValueEncoder,
};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingArray, EncodingScalar, TermDecoder, TermEncoder, TermEncoding};
use rdf_fusion_model::{Decimal, Integer, Numeric, NumericPair, ThinError, ThinResult};
use std::ops::Div;
use std::sync::Arc;

pub fn avg_typed_value() -> Arc<AggregateUDF> {
    Arc::new(create_udaf(
        BuiltinName::Avg.to_string().as_str(),
        vec![TypedValueEncoding::data_type()],
        Arc::new(TypedValueEncoding::data_type()),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(SparqlAvg::new()))),
        Arc::new(vec![TypedValueEncoding::data_type(), DataType::UInt64]),
    ))
}

#[derive(Debug)]
struct SparqlAvg {
    sum: ThinResult<Numeric>,
    count: u64,
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
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.is_empty() || self.sum.is_err() {
            return Ok(());
        }
        let arr = TypedValueEncoding::try_new_array(Arc::clone(&values[0]))?;
        let arr_len = u64::try_from(arr.array().len())
            .map_err(|_| exec_datafusion_err!("Array was too large."))?;
        self.count += arr_len;

        for value in NumericTermValueDecoder::decode_terms(&arr) {
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
                } else {
                    self.sum = ThinError::expected();
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.count == 0 {
            let count = i64::try_from(self.count)
                .map_err(|_| exec_datafusion_err!("Count too large for current xsd::Integer"))?;
            return IntegerTermValueEncoder::encode_term(Ok(Integer::from(count)))
                .map(EncodingScalar::into_scalar_value);
        }

        let Ok(sum) = self.sum else {
            return IntegerTermValueEncoder::encode_term(ThinError::expected())
                .map(EncodingScalar::into_scalar_value);
        };

        let count = Numeric::Decimal(Decimal::from(self.count));
        let result = match NumericPair::with_casts_from(sum, count) {
            NumericPair::Int(_, _) => unreachable!("Starts with Integer"),
            NumericPair::Integer(lhs, rhs) => {
                let value = lhs.checked_div(rhs);
                IntegerTermValueEncoder::encode_term(value).map(EncodingScalar::into_scalar_value)
            }
            NumericPair::Float(lhs, rhs) => {
                let value = lhs.div(rhs);
                FloatTermValueEncoder::encode_term(Ok(value)).map(EncodingScalar::into_scalar_value)
            }
            NumericPair::Double(lhs, rhs) => {
                let value = lhs.div(rhs);
                DoubleTermValueEncoder::encode_term(Ok(value))
                    .map(EncodingScalar::into_scalar_value)
            }
            NumericPair::Decimal(lhs, rhs) => {
                let value = lhs.checked_div(rhs);
                DecimalTermValueEncoder::encode_term(value).map(EncodingScalar::into_scalar_value)
            }
        }?;
        Ok(result)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let value = NumericTypedValueEncoder::encode_term(self.sum)?;
        Ok(vec![
            value.into_scalar_value(),
            ScalarValue::UInt64(Some(self.count)),
        ])
    }

    #[allow(clippy::missing_asserts_for_indexing)]
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let arr = TypedValueEncoding::try_new_array(Arc::clone(&states[0]))?;
        let counts = states[1].as_primitive::<UInt64Type>();
        for (count, value) in counts
            .values()
            .iter()
            .zip(NumericTermValueDecoder::decode_terms(&arr))
        {
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
                } else {
                    self.sum = ThinError::expected();
                }
            }
            self.count += *count;
        }

        Ok(())
    }
}
