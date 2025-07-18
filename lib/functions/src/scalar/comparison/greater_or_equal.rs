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

/// Implementation of the SPARQL `>=` operator.
#[derive(Debug)]
pub struct GreaterOrEqualSparqlOp {}

impl Default for GreaterOrEqualSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl GreaterOrEqualSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::GreaterOrEqual);

    /// Creates a new [GreaterOrEqualSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for GreaterOrEqualSparqlOp {
    type Args<TEncoding: TermEncoding> = BinaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn supported_encodings(&self) -> &[EncodingName] {
        &[EncodingName::TypedValue]
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

    fn invoke_typed_value_encoding(
        &self,
        BinaryArgs(lhs, rhs): Self::Args<TypedValueEncoding>,
    ) -> DFResult<ColumnarValue> {
        dispatch_binary_typed_value(
            &lhs,
            &rhs,
            |lhs_value, rhs_value| {
                lhs_value
                    .partial_cmp(&rhs_value)
                    .map(|o| [Ordering::Equal, Ordering::Greater].contains(&o))
                    .map(Into::into)
                    .map(TypedValueRef::BooleanLiteral)
                    .ok_or(ThinError::Expected)
            },
            |_, _| ThinError::expected(),
        )
    }
}
