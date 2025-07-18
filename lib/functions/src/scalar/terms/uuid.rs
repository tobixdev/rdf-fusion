use crate::builtin::BuiltinName;
use crate::scalar::{NullaryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingArray, EncodingName, TermEncoder, TermEncoding};
use rdf_fusion_model::{NamedNode, TypedValue};
use uuid::Uuid;

#[derive(Debug)]
pub struct UuidSparqlOp;

impl Default for UuidSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl UuidSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Uuid);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for UuidSparqlOp {
    type Args<TEncoding: TermEncoding> = NullaryArgs;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Volatile
    }

    fn return_type(&self, _target_encoding: Option<EncodingName>) -> DFResult<DataType> {
        Ok(TypedValueEncoding::data_type())
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn Fn(Self::Args<TypedValueEncoding>) -> DFResult<ColumnarValue>>> {
        Some(Box::new(|NullaryArgs { number_rows }| {
            let values = (0..number_rows)
                .map(|_| {
                    let formatted = format!("urn:uuid:{}", Uuid::new_v4());
                    TypedValue::NamedNode(NamedNode::new_unchecked(formatted))
                })
                .collect::<Vec<_>>();
            let array = DefaultTypedValueEncoder::encode_terms(
                values.iter().map(|result| Ok(result.as_ref())),
            )?;
            Ok(ColumnarValue::Array(array.into_array()))
        }))
    }
}
