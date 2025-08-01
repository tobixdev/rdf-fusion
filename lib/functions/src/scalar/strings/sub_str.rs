use crate::scalar::dispatch::{
    dispatch_binary_typed_value, dispatch_ternary_typed_value,
};
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{BinaryArgs, BinaryOrTernaryArgs, ScalarSparqlOp, TernaryArgs};
use datafusion::logical_expr::Volatility;
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{
    Integer, LanguageStringRef, SimpleLiteralRef, StringLiteralRef, ThinError,
    ThinResult, TypedValueRef,
};

/// Implementation of the SPARQL `substr` function (binary version).
#[derive(Debug)]
pub struct SubStrSparqlOp;

impl Default for SubStrSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl SubStrSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::SubStr);

    /// Creates a new [SubStrSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for SubStrSparqlOp {
    type Args<TEncoding: TermEncoding> = BinaryOrTernaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(|args| match args {
            BinaryOrTernaryArgs::Binary(BinaryArgs(lhs, rhs)) => {
                dispatch_binary_typed_value(
                    &lhs,
                    &rhs,
                    |lhs_value, rhs_value| {
                        let lhs_value = StringLiteralRef::try_from(lhs_value)?;
                        let rhs_value = Integer::try_from(rhs_value)?;
                        evaluate_substr(lhs_value, rhs_value, None)
                    },
                    |_, _| ThinError::expected(),
                )
            }
            BinaryOrTernaryArgs::Ternary(TernaryArgs(arg0, arg1, arg2)) => {
                dispatch_ternary_typed_value(
                    &arg0,
                    &arg1,
                    &arg2,
                    |arg0, arg1, arg2| {
                        let arg0 = StringLiteralRef::try_from(arg0)?;
                        let arg1 = Integer::try_from(arg1)?;
                        let arg2 = Integer::try_from(arg2)?;
                        evaluate_substr(arg0, arg1, Some(arg2))
                    },
                    |_, _, _| ThinError::expected(),
                )
            }
        }))
    }
}

fn evaluate_substr(
    source: StringLiteralRef<'_>,
    starting_loc: Integer,
    length: Option<Integer>,
) -> ThinResult<TypedValueRef<'_>> {
    let index = usize::try_from(starting_loc.as_i64())?;
    let length = length.map(|l| usize::try_from(l.as_i64())).transpose()?;

    // We want to slice on char indices, not byte indices
    let mut start_iter = source
        .0
        .char_indices()
        .skip(index.checked_sub(1).ok_or(ThinError::ExpectedError)?)
        .peekable();
    let result = if let Some((start_position, _)) = start_iter.peek().copied() {
        if let Some(length) = length {
            let mut end_iter = start_iter.skip(length).peekable();
            if let Some((end_position, _)) = end_iter.peek() {
                &source.0[start_position..*end_position]
            } else {
                &source.0[start_position..]
            }
        } else {
            &source.0[start_position..]
        }
    } else {
        ""
    };

    Ok(match source.1 {
        None => TypedValueRef::SimpleLiteral(SimpleLiteralRef { value: result }),
        Some(language) => TypedValueRef::LanguageStringLiteral(LanguageStringRef {
            value: result,
            language,
        }),
    })
}
