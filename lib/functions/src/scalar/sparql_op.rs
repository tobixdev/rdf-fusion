use crate::scalar::ScalarSparqlOpArgs;
use crate::scalar::sparql_op_impl::SparqlOpImpl;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::plain_term::{PLAIN_TERM_ENCODING, PlainTermEncoding};
use rdf_fusion_encoding::typed_value::{TYPED_VALUE_ENCODING, TypedValueEncoding};
use rdf_fusion_encoding::{EncodingName, RdfFusionEncodings, TermEncoding};
use std::any::Any;
use std::collections::{BTreeSet, HashSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

/// TODO
pub enum SparqlOpArity {
    Fixed(usize),
    FixedOneOf(BTreeSet<usize>),
    Variadic,
}

/// TODO
pub struct ScalarSparqlOpDetails {
    pub volatility: Volatility,
    pub num_constant_args: usize,
    pub arity: SparqlOpArity,
}

impl ScalarSparqlOpDetails {
    pub fn default_with_arity(arity: SparqlOpArity) -> Self {
        Self {
            volatility: Volatility::Immutable,
            num_constant_args: 0,
            arity,
        }
    }
}

/// TODO
pub trait ScalarSparqlOp: Debug + Hash + Eq + Send + Sync {
    /// TODO
    fn name(&self) -> &FunctionName;

    /// TODO
    fn details(&self) -> ScalarSparqlOpDetails;

    /// TODO
    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        None
    }

    /// TODO
    fn plain_term_encoding_op(&self) -> Option<Box<dyn SparqlOpImpl<PlainTermEncoding>>> {
        None
    }

    /// TODO
    fn object_id_encoding_op(
        &self,
        _object_id_encoding: &ObjectIdEncoding,
    ) -> Option<Box<dyn SparqlOpImpl<ObjectIdEncoding>>> {
        None
    }
}

#[derive(Debug, Eq)]
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
        let details = op.details();

        let mut type_signatures = Vec::new();
        if op.plain_term_encoding_op().is_some() {
            todo!("Implement support for plain term encodings");
        }

        if op.typed_value_encoding_op().is_some() {
            todo!("Implement support for plain term encodings");
        }

        if let Some(oid_encoding) = encodings.object_id() {
            if op.object_id_encoding_op(oid_encoding).is_some() {
                todo!("Implement support for plain term encodings");
            }
        }

        let type_signature = if type_signatures.len() == 1 {
            type_signatures.pop().unwrap()
        } else if type_signatures.is_empty() {
            TypeSignature::Variadic(vec![]) // Or handle this case as an error if no encodings are supported
        } else {
            TypeSignature::OneOf(type_signatures)
        };
        let signature = Signature::new(type_signature, details.volatility);

        Self {
            name,
            signature,
            op,
            encodings,
        }
    }

    fn detect_input_encoding(
        &self,
        arg_types: &[DataType],
    ) -> DFResult<Option<EncodingName>> {
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
                } else if let Some(oid_encoding) = self.encodings.object_id()
                    && let Some(op_impl) = self.op.object_id_encoding_op(oid_encoding)
                {
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
                exec_err!(
                    "The SparqlOp infrastructure does not support the Sortable encoding."
                )
            }
            Some(EncodingName::ObjectId) => {
                let encoding = self.encodings.object_id().ok_or(exec_datafusion_err!(
                    "Could not find the object id encoding."
                ))?;

                self
                    .op
                    .object_id_encoding_op(encoding)
                    .ok_or(exec_datafusion_err!(
                    "The SPARQL operation '{}' does not support the ObjectID encoding.",
                    &self.name
                ))
                    .map(|op_impl| op_impl.return_type())
            }
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match self.op.details().arity {
            SparqlOpArity::Fixed(expected) => {
                if args.args.len() != expected {
                    return exec_err!(
                        "Expected {} arguments, but got {}.",
                        expected,
                        args.args.len()
                    );
                }
            }
            SparqlOpArity::FixedOneOf(expected) => {
                if !expected.contains(&args.args.len()) {
                    return exec_err!(
                        "Expected any of {:?} arguments, but got {}.",
                        expected,
                        args.args.len()
                    );
                }
            }
            SparqlOpArity::Variadic => {}
        }

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
                } else if let Some(oid_encoding) = self.encodings.object_id()
                    && self.op.object_id_encoding_op(oid_encoding).is_some()
                {
                    EncodingName::ObjectId
                } else {
                    return exec_err!("No supported encodings");
                }
            }
            Some(encoding) => encoding,
        };

        match encoding {
            EncodingName::PlainTerm => {
                if let Some(op) = self.op.plain_term_encoding_op() {
                    op.invoke(prepare_args(
                        &PLAIN_TERM_ENCODING,
                        args,
                        self.op.details(),
                    )?)
                } else {
                    exec_err!("PlainTerm encoding not supported for this operation")
                }
            }
            EncodingName::TypedValue => {
                if let Some(op) = self.op.typed_value_encoding_op() {
                    op.invoke(prepare_args(
                        &TYPED_VALUE_ENCODING,
                        args,
                        self.op.details(),
                    )?)
                } else {
                    exec_err!("TypedValue encoding not supported for this operation")
                }
            }
            EncodingName::ObjectId => {
                let Some(object_id_encoding) = self.encodings.object_id() else {
                    return exec_err!("Object ID is not registered.");
                };

                if let Some(op) = self.op.object_id_encoding_op(object_id_encoding) {
                    op.invoke(prepare_args(object_id_encoding, args, self.op.details())?)
                } else {
                    exec_err!("TypedValue encoding not supported for this operation")
                }
            }
            EncodingName::Sortable => exec_err!("Not supported"),
        }
    }
}

fn prepare_args<TEncoding: TermEncoding>(
    encoding: &TEncoding,
    args: ScalarFunctionArgs,
    _details: ScalarSparqlOpDetails,
) -> DFResult<ScalarSparqlOpArgs<TEncoding>> {
    let new_args = args
        .args
        .into_iter()
        .map(|cv| encoding.try_new_datum(cv, args.number_rows))
        .collect::<DFResult<Vec<_>>>()?;

    Ok(ScalarSparqlOpArgs {
        number_rows: args.number_rows,
        args: new_args,
    })
}

/// While it would be possible to create two different SparqlOpAdapters for the same
/// [ScalarSparqlOp] that are not identical (different encodings), this situation is unlikely to
/// happen when using RDF Fusion "normally". Therefore, we only hash the contained [ScalarSparqlOp].
impl<TScalarSparqlOp: ScalarSparqlOp> Hash for ScalarSparqlOpAdapter<TScalarSparqlOp> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.op.hash(state);
    }
}

impl<TScalarSparqlOp: ScalarSparqlOp> PartialEq
    for ScalarSparqlOpAdapter<TScalarSparqlOp>
{
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op && self.encodings == other.encodings
    }
}
