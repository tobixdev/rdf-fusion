use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::{ScalarSparqlOp, SparqlOpSignature, UnaryArgs, UnarySparqlOpSignature};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{Decimal, Numeric, ThinError, TypedValueRef};

#[derive(Debug)]
pub struct CastDecimalSparqlOp;

impl Default for CastDecimalSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastDecimalSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastDecimal);
    const SIGNATURE: UnarySparqlOpSignature = UnarySparqlOpSignature;

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastDecimalSparqlOp {
    type Encoding = TypedValueEncoding;
    type Signature = UnarySparqlOpSignature;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> &Self::Signature {
        &Self::SIGNATURE
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn return_type(&self, target_encoding: Option<EncodingName>) -> DFResult<DataType> {
        if !matches!(target_encoding, Some(EncodingName::TypedValue)) {
            return exec_err!("Unexpected target encoding: {:?}", target_encoding);
        }
        Ok(TypedValueEncoding::data_type())
    }

    fn invoke(
        &self,
        UnaryArgs(arg): <Self::Signature as SparqlOpSignature<Self::Encoding>>::Args,
    ) -> DFResult<ColumnarValue> {
        dispatch_unary_typed_value(
            &arg,
            |value| {
                let converted = match value {
                    TypedValueRef::BooleanLiteral(v) => Decimal::from(v),
                    TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
                    TypedValueRef::NumericLiteral(numeric) => match numeric {
                        Numeric::Int(v) => Decimal::from(v),
                        Numeric::Integer(v) => Decimal::from(v),
                        Numeric::Float(v) => Decimal::try_from(v)?,
                        Numeric::Double(v) => Decimal::try_from(v)?,
                        Numeric::Decimal(v) => v.into(),
                    },
                    _ => return ThinError::expected(),
                };
                Ok(TypedValueRef::NumericLiteral(Numeric::from(converted)))
            },
            || ThinError::expected(),
        )
    }
}
