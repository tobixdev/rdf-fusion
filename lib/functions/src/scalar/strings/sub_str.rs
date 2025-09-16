use crate::scalar::dispatch::{
    dispatch_binary_typed_value, dispatch_ternary_typed_value,
};
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{
    Integer, LanguageStringRef, SimpleLiteralRef, StringLiteralRef, ThinError,
    ThinResult, TypedValueRef,
};

/// Implementation of the SPARQL `substr` function (binary version).
#[derive(Debug, Hash, PartialEq, Eq)]
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
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails::default_with_arity(SparqlOpArity::FixedOneOf(
            [2, 3].into(),
        ))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            match args.args.len() {
                2 => dispatch_binary_typed_value(
                    &args.args[0],
                    &args.args[1],
                    |lhs_value, rhs_value| {
                        let lhs_value = StringLiteralRef::try_from(lhs_value)?;
                        let rhs_value = Integer::try_from(rhs_value)?;
                        evaluate_substr(lhs_value, rhs_value, None)
                    },
                    |_, _| ThinError::expected(),
                ),
                3 => dispatch_ternary_typed_value(
                    &args.args[0],
                    &args.args[1],
                    &args.args[2],
                    |arg0, arg1, arg2| {
                        let arg0 = StringLiteralRef::try_from(arg0)?;
                        let arg1 = Integer::try_from(arg1)?;
                        let arg2 = Integer::try_from(arg2)?;
                        evaluate_substr(arg0, arg1, Some(arg2))
                    },
                    |_, _, _| ThinError::expected(),
                ),
                _ => unreachable!("Invalid number of arguments"),
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
