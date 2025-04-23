use crate::encoded::dispatch::dispatch_binary;
use crate::encoded::{EncTerm, EncTermField};
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datamodel::{Boolean, RdfOpResult, TermRef};
use functions_scalar::ScalarBinaryRdfOp;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncIsCompatible {
    signature: Signature,
}

impl EncIsCompatible {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::data_type(); 2]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarBinaryRdfOp for EncIsCompatible {
    type ArgLhs<'data> = TermRef<'data>;
    type ArgRhs<'data> = TermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let is_compatible = match (lhs, rhs) {
            (TermRef::BlankNode(lhs), TermRef::BlankNode(rhs)) => lhs == rhs,
            (TermRef::NamedNode(lhs), TermRef::NamedNode(rhs)) => lhs == rhs,
            (TermRef::BooleanLiteral(lhs), TermRef::BooleanLiteral(rhs)) => lhs == rhs,
            (TermRef::NumericLiteral(lhs), TermRef::NumericLiteral(rhs)) => lhs == rhs,
            (TermRef::SimpleLiteral(lhs), TermRef::SimpleLiteral(rhs)) => lhs == rhs,
            (TermRef::LanguageStringLiteral(lhs), TermRef::LanguageStringLiteral(rhs)) => {
                lhs == rhs
            }
            (TermRef::TypedLiteral(lhs), TermRef::TypedLiteral(rhs)) => lhs == rhs,
            _ => false,
        };
        Ok(is_compatible.into())
    }

    fn evaluate_error<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
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
            _ => unreachable!("Unexpected result"),
        }
    }
}
