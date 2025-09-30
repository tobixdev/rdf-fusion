use anyhow::Context;
use datafusion::logical_expr::ScalarUDF;
use rdf_fusion::api::functions::FunctionName;
use rdf_fusion::encoding::typed_value::TypedValueEncoding;
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::functions::scalar::dispatch::dispatch_unary_typed_value;
use rdf_fusion::functions::scalar::{
    ScalarSparqlOp, ScalarSparqlOpAdapter, ScalarSparqlOpImpl, ScalarSparqlOpSignature,
    SparqlOpArity, create_typed_value_sparql_op_impl,
};
use rdf_fusion::io::{RdfFormat, RdfParser};
use rdf_fusion::model::{NamedNode, ThinError, TypedValueRef};
use rdf_fusion::store::Store;

/// This example shows how to register a custom SPARQL function that can be used by RDF Fusion.
#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    // Load data from a file.
    let store = Store::default();
    let file = std::fs::File::open("./examples/data/spiderman.ttl")
        .context("Could not find spiderman.ttl")?;
    let reader = RdfParser::from_format(RdfFormat::Turtle);
    store.load_from_reader(reader, &file).await?;

    // Register custom function.
    let context = store.context();
    context.functions().register_udf(ScalarUDF::new_from_impl(
        ScalarSparqlOpAdapter::new(
            context.encodings().clone(),
            ContainsSpidermanSparqlOp::new(),
        ),
    ));

    // Run SPARQL query.
    let query = "
    BASE <http://example.org/>
    PREFIX rel: <http://www.perceive.net/schemas/relationship/>

    SELECT ?subject ?predicate ?object
    WHERE {
        ?subject ?predicate ?object .
        FILTER(<http://example.org/containsSpiderman>(?subject))
    }
    ";
    let result = store.query(query).await?;

    // Serialize result
    let mut result_buffer = Vec::new();
    result
        .write(&mut result_buffer, QueryResultsFormat::Csv)
        .await?;
    let result = String::from_utf8(result_buffer)?;

    // Print results.
    println!("Enemies of Spiderman:");
    print!("{result}");

    Ok(())
}

/// Checks whether a given element (IRI, blank node, literal) contains the string `spiderman`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ContainsSpidermanSparqlOp {
    name: FunctionName,
}

impl Default for ContainsSpidermanSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl ContainsSpidermanSparqlOp {
    /// Creates a new [ContainsSpidermanSparqlOp].
    pub fn new() -> Self {
        Self {
            name: FunctionName::Custom(
                NamedNode::new("http://example.org/containsSpiderman").unwrap(),
            ),
        }
    }
}

impl ScalarSparqlOp for ContainsSpidermanSparqlOp {
    fn name(&self) -> &FunctionName {
        &self.name
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(1))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            // We provide some helper functions that allow you to "iterate" over the content of the
            // arrays. Note that directly operating on the array data usually can be more
            // performant. Furthermore, we may remove this API in the future.
            dispatch_unary_typed_value(
                &args.args[0],
                |value| {
                    let result = match value {
                        TypedValueRef::NamedNode(nn) => nn.as_str().contains("spiderman"),
                        TypedValueRef::SimpleLiteral(lit) => {
                            lit.value.contains("spiderman")
                        }
                        TypedValueRef::LanguageStringLiteral(lit) => {
                            lit.value.contains("spiderman")
                        }
                        _ => false,
                    };
                    Ok(TypedValueRef::BooleanLiteral(result.into()))
                },
                || ThinError::expected(),
            )
        }))
    }
}
