use datafusion::arrow::array::ArrayRef;
use datafusion::logical_expr::{AggregateUDF, Volatility, create_udaf};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use rdf_fusion_encoding::typed_value::TypedValueEncodingRef;
use rdf_fusion_encoding::typed_value::decoders::NumericTermValueDecoder;
use rdf_fusion_encoding::typed_value::encoders::NumericTypedValueEncoder;
use rdf_fusion_encoding::{EncodingScalar, TermDecoder, TermEncoder, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{Integer, Numeric, NumericPair, ThinResult};
use std::sync::Arc;

pub fn sum_typed_value(encoding: TypedValueEncodingRef) -> AggregateUDF {
    let data_type = encoding.data_type().clone();
    create_udaf(
        &BuiltinName::Sum.to_string(),
        vec![data_type.clone()],
        Arc::new(data_type.clone()),
        Volatility::Immutable,
        Arc::new(move |_| Ok(Box::new(SparqlTypedValueSum::new(Arc::clone(&encoding))))),
        Arc::new(vec![data_type.clone()]),
    )
}

#[derive(Debug)]
struct SparqlTypedValueSum {
    encoding: TypedValueEncodingRef,
    sum: ThinResult<Numeric>,
}

impl SparqlTypedValueSum {
    pub fn new(encoding: TypedValueEncodingRef) -> Self {
        SparqlTypedValueSum {
            encoding,
            sum: Ok(Numeric::Integer(Integer::from(0))),
        }
    }
}

impl Accumulator for SparqlTypedValueSum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        // TODO: Can we stop once we error?

        let arr = self.encoding.try_new_array(Arc::clone(&values[0]))?;
        for value in NumericTermValueDecoder::decode_terms(&arr) {
            if let Ok(sum) = self.sum {
                if let Ok(value) = value {
                    self.sum = match NumericPair::with_casts_from(sum, value) {
                        NumericPair::Int(lhs, rhs) => {
                            lhs.checked_add(rhs).map(Numeric::Int)
                        }
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
        NumericTypedValueEncoder::new(Arc::clone(&self.encoding))
            .encode_term(self.sum)
            .map(EncodingScalar::into_scalar_value)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let value = NumericTypedValueEncoder::new(Arc::clone(&self.encoding))
            .encode_term(self.sum)?;
        Ok(vec![value.into_scalar_value()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}
