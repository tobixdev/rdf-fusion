use crate::scalar::sparql_op_impl::ScalarSparqlOpImpl;
use crate::scalar::ScalarSparqlOpArgs;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::plain_term::{PlainTermEncoding, PLAIN_TERM_ENCODING};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, RdfFusionEncodings, TermEncoding};
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

/// Defines the arity of a SPARQL operation. In other words, the number of arguments that the
/// [ScalarSparqlOp] has.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum SparqlOpArity {
    /// No arguments.
    Nullary,
    /// A fixed number of arguments.
    ///
    /// `SparqlOpArity::Fixed(0)` is equivalent to [SparqlOpArity::Nullary].
    Fixed(usize),
    /// One of the given [SparqlOpArity].
    OneOf(Vec<SparqlOpArity>),
    /// Any number of arguments (including zero).
    Variadic,
}

impl SparqlOpArity {
    /// Returns a [TypeSignature] for the given [SparqlOpArity].
    pub fn type_signature<TEncoding: TermEncoding>(
        &self,
        encoding: &TEncoding,
    ) -> TypeSignature {
        match self {
            SparqlOpArity::Nullary => TypeSignature::Nullary,
            SparqlOpArity::Fixed(n) => {
                TypeSignature::Uniform(*n, vec![encoding.data_type().clone()])
            }
            SparqlOpArity::OneOf(ns) => {
                let inner = ns
                    .iter()
                    .map(|n| n.type_signature(encoding))
                    .collect::<Vec<_>>();
                TypeSignature::OneOf(inner)
            }
            SparqlOpArity::Variadic => TypeSignature::OneOf(vec![
                TypeSignature::Nullary,
                TypeSignature::Variadic(vec![encoding.data_type().clone()]),
            ]),
        }
    }
}

/// Defines further details about a [ScalarSparqlOp].
pub struct ScalarSparqlOpSignature {
    /// Whether the [ScalarSparqlOp] is volatile. See [Volatility] for more information.
    pub volatility: Volatility,
    /// The [SparqlOpArity] of the [ScalarSparqlOp].
    pub arity: SparqlOpArity,
}

impl ScalarSparqlOpSignature {
    /// Returns a [ScalarSparqlOpSignature] with the given arity and [Volatility::Immutable].
    pub fn default_with_arity(arity: SparqlOpArity) -> Self {
        Self {
            volatility: Volatility::Immutable,
            arity,
        }
    }
}

/// A [ScalarSparqlOp] is a function that operates on RDF terms. The function may return a different
/// type of value. For example, a function that takes two RDF terms and outputs an integer can be
/// implemented using this trait.
///
/// The goal is to make it easier for users to implement custom SPARQL functions. The different
/// encodings of RDF Fusion are handled by providing a [ScalarSparqlOpImpl] for any given encoding.
///
/// To install a [ScalarSparqlOp] in DataFusion, use the [ScalarSparqlOpAdapter]. The adapter will
/// mediate between DataFusion's API and the given [ScalarSparqlOpImpl].
pub trait ScalarSparqlOp: Debug + Hash + Eq + Send + Sync {
    /// Returns the name of the operation.
    fn name(&self) -> &FunctionName;

    /// Returns the signature of this operation.
    fn signature(&self) -> ScalarSparqlOpSignature;

    /// Returns the [ScalarSparqlOpImpl] for the [TypedValueEncoding].
    ///
    /// If [None] is returned, the operation does not support the [TypedValueEncoding].
    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        None
    }

    /// Returns the [ScalarSparqlOpImpl] for the [PlainTermEncoding].
    ///
    /// If [None] is returned, the operation does not support the [PlainTermEncoding].
    fn plain_term_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<PlainTermEncoding>>> {
        None
    }

    /// Returns the [ScalarSparqlOpImpl] for the [ObjectIdEncoding].
    ///
    /// If [None] is returned, the operation does not support the [ObjectIdEncoding].
    fn object_id_encoding_op(
        &self,
        _object_id_encoding: &ObjectIdEncoding,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<ObjectIdEncoding>>> {
        None
    }
}

/// Mediates between DataFusion's API and a [ScalarSparqlOp].
///
/// This includes the following tasks:
/// - Set up the argument types of the UDFs depending on the supported encodings
/// - Set up the argument types of the UDFs depending on the configured encodings in the engine
/// - Detecting the used input encoding and calling the correct [ScalarSparqlOpImpl].
#[derive(Debug, Eq)]
pub struct ScalarSparqlOpAdapter<TScalarSparqlOp: ScalarSparqlOp> {
    /// The stringified name of the [ScalarSparqlOp].
    name: String,
    /// The DataFusion [Signature] of the [ScalarSparqlOp].
    signature: Signature,
    /// The instance of the [ScalarSparqlOp].
    op: TScalarSparqlOp,
    /// The configured [RdfFusionEncodings] in the engine.
    encodings: RdfFusionEncodings,
}

impl<TScalarSparqlOp: ScalarSparqlOp> ScalarSparqlOpAdapter<TScalarSparqlOp> {
    /// Creates a new adapter for the given `op`.
    pub fn new(encodings: RdfFusionEncodings, op: TScalarSparqlOp) -> Self {
        let name = op.name().to_string();
        let details = op.signature();

        let mut type_signatures = Vec::new();
        if op.plain_term_encoding_op().is_some() {
            let type_signature = details
                .arity
                .type_signature(encodings.plain_term().as_ref());
            type_signatures.push(type_signature);
        }

        if op.typed_value_encoding_op().is_some() {
            let type_signature = details
                .arity
                .type_signature(encodings.typed_value().as_ref());
            type_signatures.push(type_signature);
        }

        if let Some(oid_encoding) = encodings.object_id() {
            if op.object_id_encoding_op(oid_encoding).is_some() {
                let type_signature = details.arity.type_signature(oid_encoding.as_ref());
                type_signatures.push(type_signature);
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

    fn prepare_args<TEncoding: TermEncoding>(
        &self,
        encoding: &TEncoding,
        args: ScalarFunctionArgs,
    ) -> DFResult<ScalarSparqlOpArgs<TEncoding>> {
        let sparql_args = args
            .args
            .into_iter()
            .map(|cv| encoding.try_new_datum(cv, args.number_rows))
            .collect::<DFResult<Vec<_>>>()?;

        Ok(ScalarSparqlOpArgs {
            number_rows: args.number_rows,
            args: sparql_args,
        })
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
                    op.invoke(self.prepare_args(&PLAIN_TERM_ENCODING, args)?)
                } else {
                    exec_err!("PlainTerm encoding not supported for this operation")
                }
            }
            EncodingName::TypedValue => {
                if let Some(op) = self.op.typed_value_encoding_op() {
                    op.invoke(self.prepare_args(&TYPED_VALUE_ENCODING, args)?)
                } else {
                    exec_err!("TypedValue encoding not supported for this operation")
                }
            }
            EncodingName::ObjectId => {
                let Some(object_id_encoding) = self.encodings.object_id() else {
                    return exec_err!("Object ID is not registered.");
                };

                if let Some(op) = self.op.object_id_encoding_op(object_id_encoding) {
                    op.invoke(self.prepare_args(object_id_encoding, args)?)
                } else {
                    exec_err!("TypedValue encoding not supported for this operation")
                }
            }
            EncodingName::Sortable => exec_err!("Not supported"),
        }
    }
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
