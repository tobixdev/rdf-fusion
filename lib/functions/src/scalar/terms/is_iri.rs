use crate::builtin::BuiltinName;
use crate::scalar::{ScalarSparqlOp, SparqlOpSignature, UnarySparqlOpArgs, UnarySparqlOpSignature};
use crate::FunctionName;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::{
    TypedValueArray, TypedValueArrayBuilder, TypedValueEncoding, TypedValueEncodingField,
};
use rdf_fusion_encoding::{EncodingDatum, EncodingName, EncodingScalar, TermEncoding};

/// TODO
#[derive(Debug)]
pub struct IsIriSparqlOp {}

impl Default for IsIriSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsIriSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::IsIri);
    const SIGNATURE: UnarySparqlOpSignature = UnarySparqlOpSignature;

    /// Creates a new [IsIriSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for IsIriSparqlOp {
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
        UnarySparqlOpArgs(arg): <Self::Signature as SparqlOpSignature<Self::Encoding>>::Args,
    ) -> DFResult<ColumnarValue> {
        match arg {
            EncodingDatum::Array(array) => {
                let array = invoke_typed_value_array(array)?;
                Ok(ColumnarValue::Array(array))
            }
            EncodingDatum::Scalar(scalar, _) => {
                let array = scalar.to_array(1)?;
                let array_result = invoke_typed_value_array(array)?;
                let scalar_result = ScalarValue::try_from_array(&array_result, 0)?;
                Ok(ColumnarValue::Scalar(scalar_result))
            }
        }
    }
}

fn invoke_typed_value_array(array: TypedValueArray) -> DFResult<ArrayRef> {
    let parts = array.parts_as_ref();

    let mut result = TypedValueArrayBuilder::default();
    for type_id in parts.array.type_ids() {
        if *type_id == TypedValueEncodingField::Null.type_id() {
            result.append_null()?;
        } else {
            let boolean = *type_id == TypedValueEncodingField::NamedNode.type_id();
            result.append_boolean(boolean.into())?;
        }
    }

    Ok(result.finish())
}
