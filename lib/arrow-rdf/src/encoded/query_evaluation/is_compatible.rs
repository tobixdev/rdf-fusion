use crate::encoded::dispatch::dispatch_binary;
use crate::encoded::{EncTerm, EncTermField};
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use model::{Boolean, InternalTermRef, ThinResult};
use functions_scalar::ScalarBinaryRdfOp;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncIsCompatible {
    signature: Signature,
}

impl Default for EncIsCompatible {
    fn default() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::data_type(); 2]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarBinaryRdfOp for EncIsCompatible {
    type ArgLhs<'data> = InternalTermRef<'data>;
    type ArgRhs<'data> = InternalTermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let is_compatible = match (lhs, rhs) {
            (InternalTermRef::BlankNode(lhs), InternalTermRef::BlankNode(rhs)) => lhs == rhs,
            (InternalTermRef::NamedNode(lhs), InternalTermRef::NamedNode(rhs)) => lhs == rhs,
            (InternalTermRef::BooleanLiteral(lhs), InternalTermRef::BooleanLiteral(rhs)) => lhs == rhs,
            (InternalTermRef::NumericLiteral(lhs), InternalTermRef::NumericLiteral(rhs)) => lhs == rhs,
            (InternalTermRef::SimpleLiteral(lhs), InternalTermRef::SimpleLiteral(rhs)) => lhs == rhs,
            (InternalTermRef::LanguageStringLiteral(lhs), InternalTermRef::LanguageStringLiteral(rhs)) => {
                lhs == rhs
            }
            (InternalTermRef::TypedLiteral(lhs), InternalTermRef::TypedLiteral(rhs)) => lhs == rhs,
            _ => false,
        };
        Ok(is_compatible.into())
    }

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        // At least one side is an error, therefore the terms are compatible.
        Ok(true.into())
    }
}

impl ScalarUDFImpl for EncIsCompatible {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_is_compatible"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs<'_>,
    ) -> datafusion::common::Result<ColumnarValue> {
        let result = dispatch_binary(self, args.args.as_slice(), args.number_rows)?;
        match result {
            ColumnarValue::Array(array) => {
                let array = as_enc_term_array(array.as_ref())?;
                Ok(ColumnarValue::Array(Arc::clone(
                    array.child(EncTermField::Boolean.type_id()),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::Union(Some((_, scalar)), _, _)) => {
                Ok(ColumnarValue::Scalar(*scalar))
            }
            ColumnarValue::Scalar(_) => exec_err!("Unexpected non-union Scalar"),
        }
    }
}
