use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::{SimpleLiteralRef, ThinError, TypedValueRef};

/// Implementation of the SPARQL `langMatches` function.
#[derive(Debug)]
pub struct LangMatchesSparqlOp;

impl Default for LangMatchesSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl LangMatchesSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::LangMatches);

    /// Creates a new [LangMatchesSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for LangMatchesSparqlOp {
    type Args<TEncoding: TermEncoding> = BinaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(|BinaryArgs(lhs, rhs)| {
            dispatch_binary_typed_value(
                &lhs,
                &rhs,
                |lhs_value, rhs_value| {
                    let lhs_value = SimpleLiteralRef::try_from(lhs_value)?;
                    let rhs_value = SimpleLiteralRef::try_from(rhs_value)?;

                    let matches = if rhs_value.value == "*" {
                        !lhs_value.value.is_empty()
                    } else {
                        !ZipLongest::new(rhs_value.value.split('-'), lhs_value.value.split('-'))
                            .any(|parts| match parts {
                                (Some(range_subtag), Some(language_subtag)) => {
                                    !range_subtag.eq_ignore_ascii_case(language_subtag)
                                }
                                (Some(_), None) => true,
                                (None, _) => false,
                            })
                    };
                    Ok(TypedValueRef::BooleanLiteral(matches.into()))
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}

struct ZipLongest<T1, T2, I1: Iterator<Item = T1>, I2: Iterator<Item = T2>> {
    a: I1,
    b: I2,
}

impl<T1, T2, I1: Iterator<Item = T1>, I2: Iterator<Item = T2>> ZipLongest<T1, T2, I1, I2> {
    fn new(a: I1, b: I2) -> Self {
        Self { a, b }
    }
}

impl<T1, T2, I1: Iterator<Item = T1>, I2: Iterator<Item = T2>> Iterator
    for ZipLongest<T1, T2, I1, I2>
{
    type Item = (Option<T1>, Option<T2>);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.a.next(), self.b.next()) {
            (None, None) => None,
            r => Some(r),
        }
    }
}
