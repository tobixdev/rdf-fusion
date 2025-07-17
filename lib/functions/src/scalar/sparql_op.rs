use crate::scalar::args::SparqlOpArgs;
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;

/// TODO
pub trait ScalarSparqlOp: Debug + Send + Sync {
    /// TODO
    type Args<TEncoding: TermEncoding>: SparqlOpArgs;

    /// TODO
    fn name(&self) -> &FunctionName;

    /// TODO
    fn supported_encodings(&self) -> &[EncodingName];

    /// TODO
    fn volatility(&self) -> Volatility;

    /// TODO
    fn return_type(&self, input_encoding: Option<EncodingName>) -> DFResult<DataType>;

    /// TODO
    fn invoke_typed_value_encoding(
        &self,
        args: Self::Args<TypedValueEncoding>,
    ) -> DFResult<ColumnarValue>;

    /// TODO
    fn invoke_plain_term_encoding(
        &self,
        args: Self::Args<PlainTermEncoding>,
    ) -> DFResult<ColumnarValue> {
        exec_err!("Typed value encoding not supported by {}.", self.name())
    }
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

        let mut type_signatures = Vec::new();
        for encoding in op.supported_encodings() {
            match encoding {
                EncodingName::PlainTerm => {
                    type_signatures
                        .push(TScalarSparqlOp::Args::<PlainTermEncoding>::type_signature());
                }
                EncodingName::TypedValue => {
                    type_signatures
                        .push(TScalarSparqlOp::Args::<TypedValueEncoding>::type_signature());
                }
                EncodingName::Sortable => {
                    todo!("Not supported")
                }
            }
        }

        let volatility = op.volatility();
        let type_signature = if type_signatures.len() == 1 {
            type_signatures.pop().unwrap()
        } else {
            TypeSignature::OneOf(type_signatures)
        };
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
        let encoding_name = detect_input_encoding(arg_types)?;
        self.op.return_type(encoding_name)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let data_types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let encoding = detect_input_encoding(&data_types)?;

        let encoding = match encoding {
            None => self.op.supported_encodings().first().unwrap().clone(),
            Some(encoding) => encoding,
        };

        match encoding {
            EncodingName::PlainTerm => {
                let args = TScalarSparqlOp::Args::<PlainTermEncoding>::try_from_args(args)?;
                self.op.invoke_plain_term_encoding(args)
            }
            EncodingName::TypedValue => {
                let args = TScalarSparqlOp::Args::<TypedValueEncoding>::try_from_args(args)?;
                self.op.invoke_typed_value_encoding(args)
            }
            EncodingName::Sortable => todo!("Not supported"),
        }
    }
}

fn detect_input_encoding(arg_types: &[DataType]) -> DFResult<Option<EncodingName>> {
    let encoding_name = arg_types
        .iter()
        .map(|dt| {
            EncodingName::try_from_data_type(dt).ok_or(exec_datafusion_err!(
                "Cannot extract RDF term encoding from argument."
            ))
        })
        .collect::<DFResult<HashSet<_>>>()?;

    if encoding_name.is_empty() {
        return Ok(None);
    }

    if encoding_name.len() > 1 {
        return plan_err!("More than one RDF term encoding used for arguments.");
    }
    Ok(encoding_name.into_iter().next())
}
