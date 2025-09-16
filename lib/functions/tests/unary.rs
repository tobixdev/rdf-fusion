use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::Field;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::physical_plan::ColumnarValue;
use insta::assert_snapshot;
use rdf_fusion_api::functions::{
    BuiltinName, FunctionName, RdfFusionFunctionArgs, RdfFusionFunctionRegistry,
};
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermArray, PlainTermArrayElementBuilder,
};
use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings, TermDecoder, TermEncoding};
use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{BlankNode, Literal, NamedNode, Term};
use std::sync::Arc;

#[test]
fn test_unary_functions_plain_term() {
    let encodings = RdfFusionEncodings::new(
        PLAIN_TERM_ENCODING,
        TYPED_VALUE_ENCODING,
        None,
        SORTABLE_TERM_ENCODING,
    );
    let registry = DefaultRdfFusionFunctionRegistry::new(encodings);
    let test_vector = create_plain_term_test_vector();

    assert_snapshot!(
        "STR(PLAIN_TERM)",
        invoke_udf(&registry, BuiltinName::Str, test_vector.into_array()),
    )
}

fn invoke_udf(
    registry: &dyn RdfFusionFunctionRegistry,
    name: BuiltinName,
    arg: Arc<dyn Array>,
) -> String {
    let udf = registry
        .create_udf(FunctionName::Builtin(name), RdfFusionFunctionArgs::empty())
        .unwrap();

    let result = udf
        .invoke_with_args(ScalarFunctionArgs {
            arg_fields: vec![Arc::new(Field::new("arg", arg.data_type().clone(), true))],
            number_rows: arg.len(),
            return_field: Arc::new(Field::new(
                "result",
                PLAIN_TERM_ENCODING.data_type(),
                true,
            )),
            args: vec![ColumnarValue::Array(arg)],
        })
        .unwrap();

    let array = match result {
        ColumnarValue::Array(array) => array,
        ColumnarValue::Scalar(_) => panic!("Unexpected result type"),
    };

    let array = PLAIN_TERM_ENCODING.try_new_array(array).unwrap();
    let terms = DefaultPlainTermDecoder::decode_terms(&array)
        .map(|t| t.ok().map(|t| t.into_owned()))
        .collect::<Vec<_>>();

    let mut result = String::from("[\n");
    for term in terms {
        result.push_str(&format!("  {},\n", term.unwrap().to_string()));
    }
    result.push_str("]");

    result
}

fn create_plain_term_test_vector() -> PlainTermArray {
    let test_vector = create_test_vector();

    let mut builder = PlainTermArrayElementBuilder::new(test_vector.len());
    for term in test_vector {
        builder.append_term(term.as_ref());
    }

    builder.finish()
}

fn create_test_vector() -> Vec<Term> {
    vec![
        Term::NamedNode(NamedNode::new("http://example.org/foo").unwrap()),
        Term::BlankNode(BlankNode::new("myBlank").unwrap()),
        Term::BlankNode(BlankNode::new("4e15c8ff3a72491a92c64e7415a11d90").unwrap()),
        Term::Literal(Literal::new_simple_literal("")), // Empty String
        Term::Literal(Literal::new_simple_literal("Plain Literal")),
        Term::Literal(Literal::new_simple_literal(
            "🤖🦀🤖 Bee Boo Boo Ba Bee Bee 🤖🦀🤖",
        )),
        Term::Literal(Literal::new_language_tagged_literal("Äpfel", "de-at").unwrap()),
        Term::Literal(Literal::new_language_tagged_literal("привіт", "uk-ukr").unwrap()),
        Term::Literal(Literal::new_typed_literal("true", xsd::BOOLEAN)),
        Term::Literal(Literal::new_typed_literal("false", xsd::BOOLEAN)),
        Term::Literal(Literal::new_typed_literal("10", xsd::INT)),
        Term::Literal(Literal::new_typed_literal("010", xsd::INT)),
        Term::Literal(Literal::new_typed_literal("0", xsd::INT)),
        Term::Literal(Literal::new_typed_literal("10", xsd::INTEGER)),
        Term::Literal(Literal::new_typed_literal("010", xsd::INTEGER)),
        Term::Literal(Literal::new_typed_literal("0", xsd::INTEGER)),
        Term::Literal(Literal::new_typed_literal("10", xsd::FLOAT)),
        Term::Literal(Literal::new_typed_literal("10.0", xsd::FLOAT)),
        Term::Literal(Literal::new_typed_literal("0", xsd::FLOAT)),
        Term::Literal(Literal::new_typed_literal("10", xsd::DOUBLE)),
        Term::Literal(Literal::new_typed_literal("10.0", xsd::DOUBLE)),
        Term::Literal(Literal::new_typed_literal("0", xsd::DOUBLE)),
        Term::Literal(Literal::new_typed_literal("10", xsd::DECIMAL)),
        Term::Literal(Literal::new_typed_literal("10.0", xsd::DECIMAL)),
        Term::Literal(Literal::new_typed_literal("0", xsd::DECIMAL)),
    ]
}
