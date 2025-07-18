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
use rdf_fusion_model::{CompatibleStringArgs, StringLiteralRef, ThinError, TypedValueRef};

/// Implementation of the SPARQL `contains` function.
#[derive(Debug)]
pub struct ContainsSparqlOp {}

impl Default for ContainsSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl ContainsSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Contains);

    /// Creates a new [ContainsSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for ContainsSparqlOp {
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
                let lhs_value = StringLiteralRef::try_from(lhs_value)?;
                let rhs_value = StringLiteralRef::try_from(rhs_value)?;
                let args = CompatibleStringArgs::try_from(lhs_value, rhs_value)?;
                Ok(TypedValueRef::BooleanLiteral(args.lhs.contains(args.rhs).into()))
            },
            |_, _| ThinError::expected(),
        )
    }
}
