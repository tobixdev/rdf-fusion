use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use crate::FunctionName;
use datafusion::logical_expr::Volatility;
use md5::{Digest, Md5};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::{SimpleLiteral, ThinError, TypedValue, TypedValueRef};

#[derive(Debug)]
pub struct Md5SparqlOp;

impl Default for Md5SparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Md5SparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Md5);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for Md5SparqlOp {
    type Args<TEncoding: TermEncoding> = UnaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(|UnaryArgs(arg)| {
            dispatch_unary_owned_typed_value(
                &arg,
                |value| {
                    let string = match value {
                        TypedValueRef::SimpleLiteral(value) => value.value,
                        TypedValueRef::LanguageStringLiteral(value) => value.value,
                        _ => return ThinError::expected(),
                    };

                    let mut hasher = Md5::new();
                    hasher.update(string);
                    let result = hasher.finalize();
                    let value = format!("{result:x}");

                    Ok(TypedValue::SimpleLiteral(SimpleLiteral { value }))
                },
                ThinError::expected,
            )
        }))
    }
}
