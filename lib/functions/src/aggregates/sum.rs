use std::collections::HashMap;
use crate::{DFResult, FunctionName};
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use graphfusion_encoding::typed_value::decoders::NumericTermValueDecoder;
use graphfusion_encoding::typed_value::encoders::NumericTypedValueEncoder;
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_encoding::{EncodingName, EncodingScalar, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_model::{Integer, Numeric, NumericPair, Term, ThinResult};
use std::sync::{Arc, LazyLock};
use crate::aggregates::ENC_AVG;
use crate::builtin::BuiltinName;
use crate::builtin::factory::GraphFusionUdafFactory;

pub static ENC_SUM: LazyLock<AggregateUDF> = LazyLock::new(|| {
    create_udaf(
        "enc_sum",
        vec![TypedValueEncoding::data_type()],
        Arc::new(TypedValueEncoding::data_type()),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(SparqlSum::new()))),
        Arc::new(vec![TypedValueEncoding::data_type()]),
    )
});

#[derive(Debug)]
pub struct SparqlSumUdafFactory {}

impl GraphFusionUdafFactory for SparqlSumUdafFactory {
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
struct SparqlSum {
    sum: ThinResult<Numeric>,
}

impl SparqlSum {
    pub fn new() -> Self {
        SparqlSum {
            sum: Ok(Numeric::Integer(Integer::from(0))),
        }
    }
}

impl Accumulator for SparqlSum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        // TODO: Can we stop once we error?

        let arr = TypedValueEncoding::try_new_array(values[0].clone())?;
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
                }
            }
        }

        Ok(())
    }

    // DataFusion expects this function to return the final value of this aggregator.
    // in this case, this is the formula of the geometric mean
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        NumericTypedValueEncoder::encode_term(self.sum).map(EncodingScalar::into_scalar_value)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let value = NumericTypedValueEncoder::encode_term(self.sum)?;
        Ok(vec![value.into_scalar_value()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
}
