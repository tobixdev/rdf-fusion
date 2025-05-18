use crate::builtin::BuiltinName;
use crate::FunctionName;
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::plain_term::encoders::BooleanPlainTermEncoder;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_functions_scalar::SameTermSparqlOp;

// Comparisons
impl_binary_sparql_op!(
    PlainTermEncoding,
    DefaultPlainTermDecoder,
    DefaultPlainTermDecoder,
    BooleanPlainTermEncoder,
    same_term,
    SameTermSparqlOp,
    FunctionName::Builtin(BuiltinName::SameTerm)
);
