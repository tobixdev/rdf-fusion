use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::{ScalarSparqlOp, SparqlOpSignature, UnaryArgs, UnarySparqlOpSignature};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{Iri, NamedNode, ThinError, TypedValue, TypedValueRef};

#[derive(Debug)]
pub struct IriSparqlOp {
    base_iri: Option<Iri<String>>,
}

impl IriSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Iri);
    const SIGNATURE: UnarySparqlOpSignature = UnarySparqlOpSignature;

    pub fn new(base_iri: Option<Iri<String>>) -> Self {
        Self { base_iri }
    }
}

impl ScalarSparqlOp for IriSparqlOp {
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
        dispatch_unary_owned_typed_value(
            &arg,
            |value| match value {
                TypedValueRef::NamedNode(named_node) => {
                    Ok(TypedValue::NamedNode(named_node.into_owned()))
                }
                TypedValueRef::SimpleLiteral(simple_literal) => {
                    let resolving_result = if let Some(base_iri) = &self.base_iri {
                        base_iri.resolve(simple_literal.value)?
                    } else {
                        Iri::parse(simple_literal.value.to_owned())?
                    };
                    Ok(TypedValue::NamedNode(NamedNode::from(resolving_result)))
                }
                _ => ThinError::expected(),
            },
            || ThinError::expected(),
        )
    }
}
