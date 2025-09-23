#![cfg(test)]
#![allow(clippy::panic_in_result_fn)]

use oxrdfio::RdfFormat;
use rdf_fusion::model::vocab::{rdf, xsd};
use rdf_fusion::model::{GraphNameRef, LiteralRef, NamedNodeRef, QuadRef};
use rdf_fusion::store::Store;
use std::error::Error;

#[allow(clippy::non_ascii_literal)]
const DATA: &str = r#"
@prefix schema: <http://schema.org/> .
@prefix wd: <http://www.wikidata.org/entity/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

wd:Q90 a schema:City ;
    schema:name "Paris"@fr , "la ville lumière"@fr ;
    schema:country wd:Q142 ;
    schema:population 2000000 ;
    schema:startDate "-300"^^xsd:gYear ;
    schema:url "https://www.paris.fr/"^^xsd:anyURI ;
    schema:postalCode "75001" .
"#;

#[allow(clippy::non_ascii_literal)]
const GRAPH_DATA: &str = r#"
@prefix schema: <http://schema.org/> .
@prefix wd: <http://www.wikidata.org/entity/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

GRAPH <http://www.wikidata.org/wiki/Special:EntityData/Q90> {
    wd:Q90 a schema:City ;
        schema:name "Paris"@fr , "la ville lumière"@fr ;
        schema:country wd:Q142 ;
        schema:population 2000000 ;
        schema:startDate "-300"^^xsd:gYear ;
        schema:url "https://www.paris.fr/"^^xsd:anyURI ;
        schema:postalCode "75001" .
}
"#;
const NUMBER_OF_TRIPLES: usize = 8;

fn quads(graph_name: impl Into<GraphNameRef<'static>>) -> Vec<QuadRef<'static>> {
    let graph_name = graph_name.into();
    let paris = NamedNodeRef::new_unchecked("http://www.wikidata.org/entity/Q90");
    let france = NamedNodeRef::new_unchecked("http://www.wikidata.org/entity/Q142");
    let city = NamedNodeRef::new_unchecked("http://schema.org/City");
    let name = NamedNodeRef::new_unchecked("http://schema.org/name");
    let country = NamedNodeRef::new_unchecked("http://schema.org/country");
    let population = NamedNodeRef::new_unchecked("http://schema.org/population");
    let start_date = NamedNodeRef::new_unchecked("http://schema.org/startDate");
    let url = NamedNodeRef::new_unchecked("http://schema.org/url");
    let postal_code = NamedNodeRef::new_unchecked("http://schema.org/postalCode");
    vec![
        QuadRef::new(paris, rdf::TYPE, city, graph_name),
        QuadRef::new(
            paris,
            name,
            LiteralRef::new_language_tagged_literal_unchecked("Paris", "fr"),
            graph_name,
        ),
        QuadRef::new(
            paris,
            name,
            LiteralRef::new_language_tagged_literal_unchecked(
                "la ville lumi\u{E8}re",
                "fr",
            ),
            graph_name,
        ),
        QuadRef::new(paris, country, france, graph_name),
        QuadRef::new(
            paris,
            population,
            LiteralRef::new_typed_literal("2000000", xsd::INTEGER),
            graph_name,
        ),
        QuadRef::new(
            paris,
            start_date,
            LiteralRef::new_typed_literal("-300", xsd::G_YEAR),
            graph_name,
        ),
        QuadRef::new(
            paris,
            url,
            LiteralRef::new_typed_literal("https://www.paris.fr/", xsd::ANY_URI),
            graph_name,
        ),
        QuadRef::new(
            paris,
            postal_code,
            LiteralRef::new_simple_literal("75001"),
            graph_name,
        ),
    ]
}

#[tokio::test]
async fn test_load_graph() -> Result<(), Box<dyn Error>> {
    let store = Store::default();
    store
        .load_from_reader(RdfFormat::Turtle, DATA.as_bytes())
        .await?;
    for q in quads(GraphNameRef::DefaultGraph) {
        assert!(store.contains(q).await?);
    }
    store.validate().await?;
    Ok(())
}

#[tokio::test]
async fn test_load_dataset() -> Result<(), Box<dyn Error>> {
    let store = Store::default();
    store
        .load_from_reader(RdfFormat::TriG, GRAPH_DATA.as_bytes())
        .await?;
    for q in quads(NamedNodeRef::new_unchecked(
        "http://www.wikidata.org/wiki/Special:EntityData/Q90",
    )) {
        assert!(store.contains(q).await?);
    }
    store.validate().await?;
    Ok(())
}

#[tokio::test]
async fn test_load_graph_generates_new_blank_nodes() -> Result<(), Box<dyn Error>> {
    let store = Store::default();
    for _ in 0..2 {
        store
            .load_from_reader(
                RdfFormat::NTriples,
                "_:a <http://example.com/p> <http://example.com/p> .".as_bytes(),
            )
            .await?;
    }
    assert_eq!(store.len().await?, 2);
    Ok(())
}

#[tokio::test]
async fn test_dump_graph() -> Result<(), Box<dyn Error>> {
    let store = Store::default();
    for q in quads(GraphNameRef::DefaultGraph) {
        store.insert(q).await?;
    }

    let mut buffer = Vec::new();
    store
        .dump_graph_to_writer(
            GraphNameRef::DefaultGraph,
            RdfFormat::NTriples,
            &mut buffer,
        )
        .await?;
    assert_eq!(
        buffer.into_iter().filter(|c| *c == b'\n').count(),
        NUMBER_OF_TRIPLES
    );
    Ok(())
}

#[tokio::test]
async fn test_dump_dataset() -> Result<(), Box<dyn Error>> {
    let store = Store::default();
    for q in quads(GraphNameRef::DefaultGraph) {
        store.insert(q).await?;
    }

    let buffer = store.dump_to_writer(RdfFormat::NQuads, Vec::new()).await?;
    assert_eq!(
        buffer.into_iter().filter(|c| *c == b'\n').count(),
        NUMBER_OF_TRIPLES
    );
    Ok(())
}

#[tokio::test]
#[ignore = "Currently we lock the entire storage for snapshotting, so this test dead locks."]
async fn test_snapshot_isolation_iterator() -> Result<(), Box<dyn Error>> {
    let quad = QuadRef::new(
        NamedNodeRef::new("http://example.com/s")?,
        NamedNodeRef::new("http://example.com/p")?,
        NamedNodeRef::new("http://example.com/o")?,
        NamedNodeRef::new("http://www.wikidata.org/wiki/Special:EntityData/Q90")?,
    );
    let store = Store::default();
    store.insert(quad).await?;
    let iter = store.stream().await.unwrap();
    store.remove(quad).await?;
    assert_eq!(iter.try_collect_to_vec().await?, vec![quad.into_owned()]);
    store.validate().await?;
    Ok(())
}
