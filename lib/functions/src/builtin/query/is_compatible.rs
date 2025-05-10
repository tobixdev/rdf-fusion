use crate::builtin::{BuiltinName, GraphFusionBuiltinFactory};
use crate::DFResult;
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use graphfusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use graphfusion_encoding::plain_term::PlainTermEncoding;
use graphfusion_encoding::{EncodingName, TermDecoder, TermEncoding};
use graphfusion_model::{Term, TermRef, ThinError, ThinResult};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
struct IsCompatibleFactory;

impl GraphFusionBuiltinFactory for IsCompatibleFactory {
    fn name(&self) -> BuiltinName {
        BuiltinName::IsCompatible
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::PlainTerm]
    }

    fn create_with_args(&self, _constant_args: HashMap<String, Term>) -> DFResult<ScalarUDF> {
        Ok(ScalarUDF::new_from_impl(IsCompatible::new(self.name())))
    }
}

#[derive(Debug)]
pub struct IsCompatible {
    name: String,
    signature: Signature,
}

impl IsCompatible {
    pub fn new(name: BuiltinName) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::new(
                TypeSignature::Exact(vec![PlainTermEncoding::data_type(); 2]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for IsCompatible {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
        match TryInto::<[_; 2]>::try_into(args.args) {
            Ok([ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)]) => {
                dispatch_binary_array_array(
                    &PlainTermEncoding::try_new_array(lhs)?,
                    &PlainTermEncoding::try_new_array(rhs)?,
                )
            }
            Ok([ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)]) => {
                dispatch_binary_scalar_array(
                    &PlainTermEncoding::try_new_scalar(lhs.clone())?,
                    &PlainTermEncoding::try_new_array(rhs.clone())?,
                )
            }
            Ok([ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)]) => {
                dispatch_binary_array_scalar(
                    &PlainTermEncoding::try_new_array(lhs.clone())?,
                    &PlainTermEncoding::try_new_scalar(rhs.clone())?,
                )
            }
            Ok([ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)]) => {
                dispatch_binary_scalar_scalar(
                    &PlainTermEncoding::try_new_scalar(lhs)?,
                    &PlainTermEncoding::try_new_scalar(rhs.clone())?,
                )
            }
            _ => exec_err!("Invalid arguments for IsCompatible"),
        }
    }
}

pub(crate) fn dispatch_binary_array_array(
    lhs: &<PlainTermEncoding as TermEncoding>::Array,
    rhs: &<PlainTermEncoding as TermEncoding>::Array,
) -> DFResult<ColumnarValue> {
    let lhs = DefaultPlainTermDecoder::decode_terms(lhs);
    let rhs = DefaultPlainTermDecoder::decode_terms(rhs);

    let results = lhs
        .zip(rhs)
        .map(|(lhs_value, rhs_value)| check_compatibility(lhs_value, rhs_value).map(|r| Some(r)))
        .collect::<Result<BooleanArray, ThinError>>();

    match results {
        Ok(result) => Ok(ColumnarValue::Array(Arc::new(result))),
        Err(ThinError::Expected) => {
            unreachable!("Should not happen. Remove after refactoring ThinResult")
        }
        Err(ThinError::InternalError(err)) => {
            exec_err!("Error while checking compatibility: {}", err)
        }
    }
}

pub(crate) fn dispatch_binary_scalar_array(
    lhs: &<PlainTermEncoding as TermEncoding>::Scalar,
    rhs: &<PlainTermEncoding as TermEncoding>::Array,
) -> DFResult<ColumnarValue> {
    let lhs_value = DefaultPlainTermDecoder::decode_term(lhs);

    let results = DefaultPlainTermDecoder::decode_terms(rhs)
        .map(|rhs_value| check_compatibility(lhs_value, rhs_value).map(|r| Some(r)))
        .collect::<Result<BooleanArray, ThinError>>();

    match results {
        Ok(result) => Ok(ColumnarValue::Array(Arc::new(result))),
        Err(ThinError::Expected) => {
            unreachable!("Should not happen. Remove after refactoring ThinResult")
        }
        Err(ThinError::InternalError(err)) => {
            exec_err!("Error while checking compatibility: {}", err)
        }
    }
}

pub(crate) fn dispatch_binary_array_scalar(
    lhs: &<PlainTermEncoding as TermEncoding>::Array,
    rhs: &<PlainTermEncoding as TermEncoding>::Scalar,
) -> DFResult<ColumnarValue> {
    let rhs_value = DefaultPlainTermDecoder::decode_term(rhs);

    let results = DefaultPlainTermDecoder::decode_terms(lhs)
        .map(|lhs_value| check_compatibility(lhs_value, rhs_value).map(|r| Some(r)))
        .collect::<Result<BooleanArray, ThinError>>();

    match results {
        Ok(result) => Ok(ColumnarValue::Array(Arc::new(result))),
        Err(ThinError::Expected) => {
            unreachable!("Should not happen. Remove after refactoring ThinResult")
        }
        Err(ThinError::InternalError(err)) => {
            exec_err!("Error while checking compatibility: {}", err)
        }
    }
}

pub(crate) fn dispatch_binary_scalar_scalar(
    lhs: &<PlainTermEncoding as TermEncoding>::Scalar,
    rhs: &<PlainTermEncoding as TermEncoding>::Scalar,
) -> DFResult<ColumnarValue> {
    let lhs = DefaultPlainTermDecoder::decode_term(lhs);
    let rhs = DefaultPlainTermDecoder::decode_term(rhs);

    match check_compatibility(lhs, rhs) {
        Ok(result) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(result)))),
        Err(ThinError::Expected) => {
            unreachable!("Should not happen. Remove after refactoring ThinResult")
        }
        Err(ThinError::InternalError(err)) => {
            exec_err!("Error while checking compatibility: {}", err)
        }
    }
}

fn check_compatibility(
    lhs: ThinResult<TermRef<'_>>,
    rhs: ThinResult<TermRef<'_>>,
) -> ThinResult<bool> {
    match (lhs, rhs) {
        (Ok(lhs_value), Ok(rhs_value)) => Ok(lhs_value == rhs_value),
        (Err(ThinError::InternalError(internal_err)), _)
        | (_, Err(ThinError::InternalError(internal_err))) => {
            ThinError::internal_error(internal_err)
        }
        _ => Ok(true),
    }
}
