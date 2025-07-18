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
use rdf_fusion_model::{LiteralRef, ThinError, TypedValueRef};

/// TODO
#[derive(Debug)]
pub struct StrDtSparqlOp {}

impl Default for StrDtSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrDtSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::StrDt);

    /// Creates a new [StrDtSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrDtSparqlOp {
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
                if let (
                    TypedValueRef::SimpleLiteral(lhs_literal),
                    TypedValueRef::NamedNode(rhs_named_node),
                ) = (lhs_value, rhs_value)
                {
                    let plain_literal =
                        LiteralRef::new_typed_literal(lhs_literal.value, rhs_named_node);
                    TypedValueRef::try_from(plain_literal).map_err(|_| ThinError::Expected)
                } else {
                    ThinError::expected()
                }
            },
            |_, _| ThinError::expected(),
        )
    }
}
