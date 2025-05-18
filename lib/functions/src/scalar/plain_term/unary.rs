use crate::builtin::BuiltinName;
use crate::{impl_unary_sparql_op, FunctionName};
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::plain_term::encoders::SimpleLiteralRefPlainTermEncoder;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_functions_scalar::StrTermOp;

// Strings
impl_unary_sparql_op!(
    PlainTermEncoding,
    DefaultPlainTermDecoder,
    SimpleLiteralRefPlainTermEncoder,
    str_plain_term,
    StrTermOp,
    FunctionName::Builtin(BuiltinName::Str)
);
