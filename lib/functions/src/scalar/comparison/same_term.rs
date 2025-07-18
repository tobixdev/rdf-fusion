use crate::builtin::BuiltinName;
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::arrow::compute::kernels::cmp::eq;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::typed_value::{TypedValueArrayBuilder, TypedValueEncoding};
use rdf_fusion_encoding::{
    EncodingArray, EncodingDatum, EncodingName, EncodingScalar, TermEncoding,
};

/// Implementation of the SPARQL `SAME_TERM` operator.
#[derive(Debug)]
pub struct SameTermSparqlOp {}

impl Default for SameTermSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl SameTermSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::SameTerm);

    /// Creates a new [SameTermSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for SameTermSparqlOp {
    type Args<TEncoding: TermEncoding> = BinaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn supported_encodings(&self) -> &[EncodingName] {
        &[EncodingName::PlainTerm]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn return_type(&self, _input_encoding: Option<EncodingName>) -> DFResult<DataType> {
        Ok(TypedValueEncoding::data_type())
    }

    fn invoke_plain_term_encoding(
        &self,
        BinaryArgs(lhs, rhs): Self::Args<PlainTermEncoding>,
    ) -> DFResult<ColumnarValue> {
        let result = match (lhs, rhs) {
            (EncodingDatum::Array(lhs), EncodingDatum::Array(rhs)) => eq(lhs.array(), rhs.array()),
            (EncodingDatum::Array(lhs), EncodingDatum::Scalar(rhs, _)) => {
                eq(lhs.array(), &rhs.scalar_value().to_scalar()?)
            }
            (EncodingDatum::Scalar(lhs, _), EncodingDatum::Array(rhs)) => {
                eq(&lhs.scalar_value().to_scalar()?, rhs.array())
            }
            (EncodingDatum::Scalar(lhs, _), EncodingDatum::Scalar(rhs, _)) => {
                let scalar_result = lhs.scalar_value() == rhs.scalar_value();
                return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                    scalar_result,
                ))));
            }
        }?;

        let mut result_builder = TypedValueArrayBuilder::default();
        for boolean in result.into_iter() {
            match boolean {
                Some(value) => result_builder.append_boolean(value.into())?,
                None => result_builder.append_null()?,
            }
        }
        Ok(ColumnarValue::Array(result_builder.finish()))
    }
}
