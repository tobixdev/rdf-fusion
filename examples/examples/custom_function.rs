use anyhow::Context;
use futures::StreamExt;
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::io::{RdfFormat, RdfParser};
use rdf_fusion::model::{GraphName, NamedNode, Quad};
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
    todo!("Register custom function");

    // Run SPARQL query.
    let query = "
    BASE <http://example.org/>
    PREFIX rel: <http://www.perceive.net/schemas/relationship/>

    SELECT ?enemy
    WHERE {
        <#spiderman> rel:enemyOf ?enemy .
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
}

/// Checks whether a given element (IRI, blank node, literal) contains the string `spiderman`.
#[derive(Debug)]
pub struct ContainsSpidermanSparqlOp;

impl Default for ContainsSpidermanSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl ContainsSpidermanSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Contains);

    /// Creates a new [ContainsSpidermanSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for ContainsSpidermanSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_binary_typed_value(
                &args.args[0],
                &args.args[1],
                |lhs_value, rhs_value| {
                    let lhs_value = StringLiteralRef::try_from(lhs_value)?;
                    let rhs_value = StringLiteralRef::try_from(rhs_value)?;
                    let args = CompatibleStringArgs::try_from(lhs_value, rhs_value)?;
                    Ok(TypedValueRef::BooleanLiteral(
                        args.lhs.contains(args.rhs).into(),
                    ))
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
