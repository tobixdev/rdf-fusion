use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::{
    NullaryArgs, NullaryOrUnaryArgs, NullaryOrUnarySparqlOpSignature, ScalarSparqlOp,
    SparqlOpSignature, UnaryArgs,
};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::{TypedValueArrayBuilder, TypedValueEncoding};
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{BlankNode, BlankNodeRef, ThinError, TypedValueRef};

#[derive(Debug)]
pub struct BNodeSparqlOp;

impl Default for BNodeSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl BNodeSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::BNode);
    const SIGNATURE: NullaryOrUnarySparqlOpSignature = NullaryOrUnarySparqlOpSignature;

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for BNodeSparqlOp {
    type Encoding = TypedValueEncoding;
    type Signature = NullaryOrUnarySparqlOpSignature;

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
        if matches!(
            target_encoding,
            Some(EncodingName::PlainTerm) | Some(EncodingName::Sortable)
        ) {
            return exec_err!("Unexpected target encoding: {:?}", target_encoding);
        }
        Ok(TypedValueEncoding::data_type())
    }

    fn invoke(
        &self,
        arg: <Self::Signature as SparqlOpSignature<Self::Encoding>>::Args,
    ) -> DFResult<ColumnarValue> {
        match arg {
            NullaryOrUnaryArgs::Nullary(NullaryArgs { number_rows }) => {
                let mut builder = TypedValueArrayBuilder::default();
                for _ in 0..number_rows {
                    builder.append_blank_node(BlankNode::default().as_ref())?;
                }
                Ok(ColumnarValue::Array(builder.finish()))
            }
            NullaryOrUnaryArgs::Unary(UnaryArgs(arg)) => dispatch_unary_typed_value(
                &arg,
                |value| match value {
                    TypedValueRef::SimpleLiteral(value) => {
                        let bnode = BlankNodeRef::new(&value.value)?;
                        Ok(TypedValueRef::BlankNode(bnode))
                    }
                    _ => ThinError::expected(),
                },
                || ThinError::expected(),
            ),
        }
    }
}
