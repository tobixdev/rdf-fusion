use anyhow::Context;
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::io::{RdfFormat, RdfParser};
use rdf_fusion::store::Store;

/// This example shows how to query RDF Fusion with SPARQL.
#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    // Load data from a file.
    let store = Store::default();
    let file = std::fs::File::open("./examples/data/spiderman.ttl")
        .context("Could not find spiderman.ttl")?;
    let reader = RdfParser::from_format(RdfFormat::Turtle);
    store.load_from_reader(reader, &file).await?;

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

    Ok(())
}
