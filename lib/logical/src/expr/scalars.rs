use crate::expr::unwrap_encoding_changes;
use datafusion::logical_expr::Expr;
use rdf_fusion_encoding::{EncodingName, RdfFusionEncodings, TermEncoding};
use rdf_fusion_model::Term;

/// Tries to extract a scalar [Term] from a given expression.
///
/// Examples (in a logical notation):
/// - `lit(<Test>)` -> `Some(<Test>)`
/// - `ENC_TV(lit(<Test>))` -> `Some(<Test>)`
/// - `col(a)` -> `None`
pub fn try_extract_scalar_term(
    encodings: &RdfFusionEncodings,
    expr: &Expr,
) -> Option<Term> {
    match unwrap_encoding_changes(expr) {
        Expr::Literal(sv, _) => {
            let encoding = encodings.try_get_encoding_name(&sv.data_type())?;
            match encoding {
                EncodingName::ObjectId => {
                    // Currently, object IDs are not supported from this function.
                    None
                }
                EncodingName::PlainTerm => {
                    let scalar = encodings
                        .plain_term()
                        .try_new_scalar(sv.clone())
                        .expect("Encoding name already validated");
                    let term = scalar.as_term().ok()?;
                    Some(term.into())
                }
                EncodingName::TypedValue => {
                    let scalar = encodings
                        .typed_value()
                        .try_new_scalar(sv.clone())
                        .expect("Encoding name already validated");
                    let typed_value = scalar.as_typed_value().ok()?;
                    Some(typed_value.into())
                }
                EncodingName::Sortable => {
                    unreachable!("Sortable encoding shoudl never create a literal")
                }
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::expr::ScalarFunction;
    use rdf_fusion_encoding::EncodingScalar;
    use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
    use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
    use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
    use rdf_fusion_extensions::functions::{
        BuiltinName, FunctionName, RdfFusionFunctionRegistry,
    };
    use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
    use rdf_fusion_model::NamedNode;

    #[test]
    fn test_plain_term_literal() {
        let enc = encodings();
        let expr = iri_literal_pt("http://example.org/test");
        let result = try_extract_scalar_term(&enc, &expr);
        assert!(result.is_some());
    }

    #[test]
    fn test_typed_value_literal() {
        let enc = encodings();
        let expr = iri_literal_tv("http://example.org/test");
        let result = try_extract_scalar_term(&enc, &expr);
        assert!(result.is_some());
    }

    #[test]
    fn test_wrapped_plain_term() {
        let enc = encodings();
        let expr = wrap_encoding(
            &enc,
            iri_literal_pt("http://example.org/test"),
            BuiltinName::WithTypedValueEncoding,
        );
        let result = try_extract_scalar_term(&enc, &expr);
        assert!(result.is_some());
    }

    #[test]
    fn test_wrapped_typed_value() {
        let enc = encodings();
        let expr = wrap_encoding(
            &enc,
            iri_literal_tv("http://example.org/test"),
            BuiltinName::WithTypedValueEncoding,
        );
        let result = try_extract_scalar_term(&enc, &expr);
        assert!(result.is_some());
    }

    #[test]
    fn test_column_returns_none() {
        let enc = encodings();
        let expr = Expr::Column(datafusion::common::Column::from_name("col"));
        let result = try_extract_scalar_term(&enc, &expr);
        assert!(result.is_none());
    }

    #[test]
    fn test_non_term_literal_returns_none() {
        let enc = encodings();
        let expr = Expr::Literal(ScalarValue::Int32(Some(42)), None);
        let result = try_extract_scalar_term(&enc, &expr);
        assert!(result.is_none());
    }

    fn iri_literal_pt(s: &str) -> Expr {
        let enc = encodings();
        let term = Term::from(NamedNode::new(s).unwrap());
        let sv = enc.plain_term().encode_term(Ok(term.as_ref())).unwrap();
        Expr::Literal(sv.into_scalar_value(), None)
    }

    fn iri_literal_tv(s: &str) -> Expr {
        let enc = encodings();
        let term = Term::from(NamedNode::new(s).unwrap());
        let sv = enc.typed_value().encode_term(Ok(term.as_ref())).unwrap();
        Expr::Literal(sv.into_scalar_value(), None)
    }

    fn wrap_encoding(
        encodings: &RdfFusionEncodings,
        expr: Expr,
        builtin: BuiltinName,
    ) -> Expr {
        let registry = DefaultRdfFusionFunctionRegistry::new(encodings.clone());
        let func = registry.udf(&FunctionName::Builtin(builtin)).unwrap();
        Expr::ScalarFunction(ScalarFunction {
            func,
            args: vec![expr],
        })
    }

    fn encodings() -> RdfFusionEncodings {
        RdfFusionEncodings::new(
            PLAIN_TERM_ENCODING,
            TYPED_VALUE_ENCODING,
            None,
            SORTABLE_TERM_ENCODING,
        )
    }
}
