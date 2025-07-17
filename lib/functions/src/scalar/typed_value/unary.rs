use crate::builtin::BuiltinName;
use crate::scalar::unary::UnaryScalarUdfOp;
use crate::{impl_unary_sparql_op, FunctionName};
use datafusion::logical_expr::ScalarUDF;
use rdf_fusion_encoding::typed_value::decoders::{
    DateTimeTermValueDecoder, DefaultTypedValueDecoder, NumericTermValueDecoder,
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use rdf_fusion_encoding::typed_value::encoders::{
    BlankNodeRefTermValueEncoder, BooleanTermValueEncoder, IntegerTermValueEncoder,
    NamedNodeRefTermValueEncoder, NamedNodeTermValueEncoder, NumericTypedValueEncoder,
    OwnedStringLiteralTermValueEncoder, SimpleLiteralRefTermValueEncoder,
};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_functions_scalar::{
    BNodeSparqlOp, BoundSparqlOp, DatatypeSparqlOp, EncodeForUriSparqlOp, IriSparqlOp,
    LCaseSparqlOp, LangSparqlOp, Md5SparqlOp, Sha1SparqlOp, Sha256SparqlOp, Sha384SparqlOp,
    Sha512SparqlOp, StrLenSparqlOp, StrTypedValueOp, TzSparqlOp, UCaseSparqlOp, UnaryMinusSparqlOp,
    UnaryPlusSparqlOp,
};
use rdf_fusion_model::Iri;
use std::sync::Arc;

// Dates and Times
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    tz_typed_value,
    TzSparqlOp,
    FunctionName::Builtin(BuiltinName::Tz)
);

// Functional Form
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder, // For Bound
    bound_typed_value,
    BoundSparqlOp,
    FunctionName::Builtin(BuiltinName::Bound)
);

// Hashing
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    md5_typed_value,
    Md5SparqlOp,
    FunctionName::Builtin(BuiltinName::Md5)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    sha1_typed_value,
    Sha1SparqlOp,
    FunctionName::Builtin(BuiltinName::Sha1)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    sha256_typed_value,
    Sha256SparqlOp,
    FunctionName::Builtin(BuiltinName::Sha256)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    sha384_typed_value,
    Sha384SparqlOp,
    FunctionName::Builtin(BuiltinName::Sha384)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    sha512_typed_value,
    Sha512SparqlOp,
    FunctionName::Builtin(BuiltinName::Sha512)
);

// Numeric
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    unary_minus_typed_value,
    UnaryMinusSparqlOp,
    FunctionName::Builtin(BuiltinName::UnaryMinus)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    unary_plus_typed_value,
    UnaryPlusSparqlOp,
    FunctionName::Builtin(BuiltinName::UnaryPlus)
);

// Strings
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    encode_for_uri_typed_value,
    EncodeForUriSparqlOp,
    FunctionName::Builtin(BuiltinName::EncodeForUri)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    lcase_typed_value,
    LCaseSparqlOp,
    FunctionName::Builtin(BuiltinName::LCase)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueEncoder,
    str_len_typed_value,
    StrLenSparqlOp,
    FunctionName::Builtin(BuiltinName::StrLen)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    ucase_typed_value,
    UCaseSparqlOp,
    FunctionName::Builtin(BuiltinName::UCase)
);

// Terms
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    BlankNodeRefTermValueEncoder,
    bnode_unary_typed_value,
    BNodeSparqlOp,
    FunctionName::Builtin(BuiltinName::BNode)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    NamedNodeRefTermValueEncoder,
    datatype_typed_value,
    DatatypeSparqlOp,
    FunctionName::Builtin(BuiltinName::Datatype)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    SimpleLiteralRefTermValueEncoder,
    lang_typed_value,
    LangSparqlOp,
    FunctionName::Builtin(BuiltinName::Lang)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    str_typed_value,
    StrTypedValueOp,
    FunctionName::Builtin(BuiltinName::Str)
);

pub fn iri_typed_value(base_iri: Option<Iri<String>>) -> Arc<ScalarUDF> {
    let op = IriSparqlOp::new(base_iri);
    let udf_impl = UnaryScalarUdfOp::<
        IriSparqlOp,
        TypedValueEncoding,
        DefaultTypedValueDecoder,
        NamedNodeTermValueEncoder,
    >::new(&FunctionName::Builtin(BuiltinName::Iri), op);
    Arc::new(ScalarUDF::new_from_impl(udf_impl))
}
