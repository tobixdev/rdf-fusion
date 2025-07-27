use crate::scalar::sparql_op_impl::SparqlOpImpl;
use crate::scalar::SparqlOpArgs;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::plain_term::{PlainTermEncoding, PLAIN_TERM_ENCODING};
use rdf_fusion_encoding::typed_value::{TypedValueEncoding, TYPED_VALUE_ENCODING};
use rdf_fusion_encoding::{EncodingName, RdfFusionEncodings, TermEncoding};
use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;

/// TODO
pub trait ScalarSparqlOp: Debug + Send + Sync {
    /// TODO
    type Args<TEncoding: TermEncoding>: SparqlOpArgs<TEncoding>;

    /// TODO
    fn name(&self) -> &FunctionName;

    /// TODO
    fn volatility(&self) -> Volatility;

    /// TODO
    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        None
    }

    /// TODO
    fn plain_term_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<PlainTermEncoding>>>> {
        None
    }

    /// TODO
    fn object_id_encoding_op(&self) -> Option<Box<dyn SparqlOpImpl<Self::Args<ObjectIdEncoding>>>> {
        None
    }
}

#[derive(Debug)]
pub struct ScalarSparqlOpAdapter<TScalarSparqlOp: ScalarSparqlOp> {
    name: String,
    signature: Signature,
    op: TScalarSparqlOp,
    encodings: RdfFusionEncodings,
}

impl<TScalarSparqlOp: ScalarSparqlOp> ScalarSparqlOpAdapter<TScalarSparqlOp> {
    /// TODO
    pub fn new(encodings: RdfFusionEncodings, op: TScalarSparqlOp) -> Self {
        let name = op.name().to_string();

        let mut type_signatures = Vec::new();
        if op.plain_term_encoding_op().is_some() {
            type_signatures.push(TScalarSparqlOp::Args::<PlainTermEncoding>::type_signature(
                &PLAIN_TERM_ENCODING,
            ));
        }
        if op.typed_value_encoding_op().is_some() {
            type_signatures.push(TScalarSparqlOp::Args::<TypedValueEncoding>::type_signature(
                &TYPED_VALUE_ENCODING,
            ));
        }
        if op.object_id_encoding_op().is_some() {
            if let Some(object_id_encoding) = &encodings.object_id() {
                type_signatures.push(TScalarSparqlOp::Args::<ObjectIdEncoding>::type_signature(
                    object_id_encoding,
                ));
            }
        }

        let volatility = op.volatility();
        let type_signature = if type_signatures.len() == 1 {
            type_signatures.pop().unwrap()
        } else if type_signatures.is_empty() {
            TypeSignature::Variadic(vec![]) // Or handle this case as an error if no encodings are supported
        } else {
            TypeSignature::OneOf(type_signatures)
        };
        let signature = Signature::new(type_signature, volatility);

        Self {
            name,
            signature,
            op,
            encodings,
        }
    }

    fn detect_input_encoding(&self, arg_types: &[DataType]) -> DFResult<Option<EncodingName>> {
        let encoding_name = arg_types
            .iter()
            .map(|dt| {
                self.encodings
                    .try_get_encoding_name(dt)
                    .ok_or(exec_datafusion_err!(
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
        let encoding_name = self.detect_input_encoding(arg_types)?;
        match encoding_name {
            None => {
                if let Some(op_impl) = self.op.plain_term_encoding_op() {
                    Ok(op_impl.return_type())
                } else if let Some(op_impl) = self.op.typed_value_encoding_op() {
                    Ok(op_impl.return_type())
                } else if let Some(op_impl) = self.op.object_id_encoding_op() {
                    Ok(op_impl.return_type())
                } else {
                    exec_err!(
                        "The SPARQL operation '{}' does not support any encoding.",
                        &self.name
                    )
                }
            }
            Some(EncodingName::PlainTerm) => self
                .op
                .plain_term_encoding_op()
                .ok_or(exec_datafusion_err!(
                    "The SPARQL operation '{}' does not support the PlainTerm encoding.",
                    &self.name
                ))
                .map(|op_impl| op_impl.return_type()),
            Some(EncodingName::TypedValue) => self
                .op
                .typed_value_encoding_op()
                .ok_or(exec_datafusion_err!(
                    "The SPARQL operation '{}' does not support the TypedValue encoding.",
                    &self.name
                ))
                .map(|op_impl| op_impl.return_type()),
            Some(EncodingName::Sortable) => {
                exec_err!("The SparqlOp infrastructure does not support the Sortable encoding.")
            }
            Some(EncodingName::ObjectId) => self
                .op
                .object_id_encoding_op()
                .ok_or(exec_datafusion_err!(
                    "The SPARQL operation '{}' does not support the ObjectID encoding.",
                    &self.name
                ))
                .map(|op_impl| op_impl.return_type()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let data_types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let encoding = self.detect_input_encoding(&data_types)?;

        let encoding = match encoding {
            None => {
                if self.op.typed_value_encoding_op().is_some() {
                    EncodingName::TypedValue
                } else if self.op.plain_term_encoding_op().is_some() {
                    EncodingName::PlainTerm
                } else if self.op.object_id_encoding_op().is_some() {
                    EncodingName::ObjectId
                } else {
                    return exec_err!("No supported encodings");
                }
            }
            Some(encoding) => encoding,
        };

        match encoding {
            EncodingName::PlainTerm => {
                let args = TScalarSparqlOp::Args::<PlainTermEncoding>::try_from_args(
                    &PLAIN_TERM_ENCODING,
                    args,
                )?;
                if let Some(op) = self.op.plain_term_encoding_op() {
                    op.invoke(args)
                } else {
                    exec_err!("PlainTerm encoding not supported for this operation")
                }
            }
            EncodingName::TypedValue => {
                let args = TScalarSparqlOp::Args::<TypedValueEncoding>::try_from_args(
                    &TYPED_VALUE_ENCODING,
                    args,
                )?;
                if let Some(op) = self.op.typed_value_encoding_op() {
                    op.invoke(args)
                } else {
                    exec_err!("TypedValue encoding not supported for this operation")
                }
            }
            EncodingName::ObjectId => {
                let Some(object_id_encoding) = self.encodings.object_id() else {
                    return exec_err!("Object ID is not registered.");
                };

                let args = TScalarSparqlOp::Args::<ObjectIdEncoding>::try_from_args(
                    object_id_encoding,
                    args,
                )?;
                if let Some(op) = self.op.object_id_encoding_op() {
                    op.invoke(args)
                } else {
                    exec_err!("TypedValue encoding not supported for this operation")
                }
            }
            EncodingName::Sortable => exec_err!("Not supported"),
        }
    }
}
