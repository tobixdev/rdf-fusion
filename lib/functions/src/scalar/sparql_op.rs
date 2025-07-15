use crate::scalar::args::SparqlOpArgs;
use crate::scalar::signature::SparqlOpSignature;
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;

/// TODO
pub trait ScalarSparqlOp: Debug + Send + Sync {
    /// TODO
    type Encoding: TermEncoding;
    /// TODO
    type Signature: SparqlOpSignature<Self::Encoding>;

    /// TODO
    fn name(&self) -> &FunctionName;

    /// TODO
    fn signature(&self) -> &Self::Signature;

    /// TODO
    fn volatility(&self) -> Volatility;

    /// TODO
    fn return_type(&self, target_encoding: Option<EncodingName>) -> DFResult<DataType>;

    /// TODO
    fn invoke(
        &self,
        args: <Self::Signature as SparqlOpSignature<Self::Encoding>>::Args,
    ) -> DFResult<ColumnarValue>;
}

#[derive(Debug)]
pub struct ScalarSparqlOpAdapter<TScalarSparqlOp: ScalarSparqlOp> {
    name: String,
    signature: Signature,
    op: TScalarSparqlOp,
}

impl<TScalarSparqlOp: ScalarSparqlOp> ScalarSparqlOpAdapter<TScalarSparqlOp> {
    /// TODO
    pub fn new(op: TScalarSparqlOp) -> Self {
        let name = op.name().to_string();

        let type_signature = op.signature().type_signature();
        let volatility = op.volatility();
        let signature = Signature::new(type_signature, volatility);

        Self {
            name,
            signature,
            op,
        }
    }
}

impl<TScalarSparqlOp: ScalarSparqlOp + 'static> ScalarUDFImpl
    for ScalarSparqlOpAdapter<TScalarSparqlOp>
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        let encoding_name = arg_types
            .iter()
            .map(|dt| {
                EncodingName::try_from_data_type(dt).ok_or(exec_datafusion_err!(
                    "Cannot extract RDF term encoding from argument."
                ))
            })
            .collect::<DFResult<HashSet<_>>>()?;

        if encoding_name.len() > 1 {
            return plan_err!("More than one RDF term encoding used for arguments.");
        }

        self.op.return_type(encoding_name.into_iter().next())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args =
            <TScalarSparqlOp::Signature as SparqlOpSignature<TScalarSparqlOp::Encoding>>::Args::try_from_args(
                args,
            )?;
        self.op.invoke(args)
    }
}
