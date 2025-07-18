use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{ThinError, TypedValueRef};
use std::cmp::Ordering;

/// Implementation of the SPARQL `>` operator.
#[derive(Debug)]
pub struct GreaterThanSparqlOp;

impl Default for GreaterThanSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl GreaterThanSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::GreaterThan);

    /// Creates a new [GreaterThanSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for GreaterThanSparqlOp {
    type Args<TEncoding: TermEncoding> = BinaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn return_type(&self, input_encoding: Option<EncodingName>) -> DFResult<DataType> {
        if !matches!(input_encoding, Some(EncodingName::TypedValue)) {
            return exec_err!("Unexpected target encoding: {:?}", input_encoding);
        }
        Ok(TypedValueEncoding::data_type())
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn Fn(Self::Args<TypedValueEncoding>) -> DFResult<ColumnarValue>>> {
        Some(Box::new(|BinaryArgs(lhs, rhs)| {
            dispatch_binary_typed_value(
                &lhs,
                &rhs,
                |lhs_value, rhs_value| {
                    lhs_value
                        .partial_cmp(&rhs_value)
                        .map(|o| o == Ordering::Greater)
                        .map(Into::into)
                        .map(TypedValueRef::BooleanLiteral)
                        .ok_or(ThinError::Expected)
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
