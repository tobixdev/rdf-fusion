use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{SimpleLiteralRef, ThinError, TypedValueRef};

/// Implementation of the SPARQL `langMatches` function.
#[derive(Debug, Hash, PartialEq, Eq)]
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
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails::default_with_arity(SparqlOpArity::Fixed(2))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_binary_typed_value(
                &args.args[0],
                &args.args[1],
                |lhs_value, rhs_value| {
                    let lhs_value = SimpleLiteralRef::try_from(lhs_value)?;
                    let rhs_value = SimpleLiteralRef::try_from(rhs_value)?;

                    let matches = if rhs_value.value == "*" {
                        !lhs_value.value.is_empty()
                    } else {
                        !ZipLongest::new(
                            rhs_value.value.split('-'),
                            lhs_value.value.split('-'),
                        )
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

impl<T1, T2, I1: Iterator<Item = T1>, I2: Iterator<Item = T2>>
    ZipLongest<T1, T2, I1, I2>
{
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
