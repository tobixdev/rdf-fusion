#![cfg(test)]

use anyhow::Result;
use rdf_fusion_testsuite::check_testsuite;

#[tokio::test]
#[ignore = "Oxigraph tests must be checked if they are still relevant"]
async fn oxigraph_sparql_testsuite() -> Result<()> {
    check_testsuite(
        "https://github.com/oxigraph/oxigraph/tests/sparql/manifest.ttl",
        &[],
    )
    .await
}

#[tokio::test]
async fn oxigraph_sparql_results_testsuite() -> Result<()> {
    check_testsuite(
        "https://github.com/oxigraph/oxigraph/tests/sparql-results/manifest.ttl",
        &[],
    )
    .await
}

#[tokio::test]
#[ignore = "GeoSPARQL not yet implemented"]
async fn oxigraph_geosparql_testsuite() -> Result<()> {
    check_testsuite(
        "https://github.com/oxigraph/oxigraph/tests/geosparql/manifest.ttl",
        &[],
    )
    .await
}
