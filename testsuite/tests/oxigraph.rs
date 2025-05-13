#![cfg(test)]

use anyhow::Result;
use rdf_fusion_testsuite::check_testsuite;

#[cfg(not(windows))] // Tests don't like git auto "\r\n" on Windows
#[tokio::test]
async fn oxigraph_parser_testsuite() -> Result<()> {
    check_testsuite(
        "https://github.com/oxigraph/oxigraph/tests/parser/manifest.ttl",
        &[],
    )
    .await
}

#[tokio::test]
async fn oxigraph_parser_recovery_testsuite() -> Result<()> {
    check_testsuite(
        "https://github.com/oxigraph/oxigraph/tests/parser-recovery/manifest.ttl",
        &[],
    )
    .await
}

#[cfg(not(windows))] // Tests don't like git auto "\r\n" on Windows
#[tokio::test]
async fn oxigraph_parser_unchecked_testsuite() -> Result<()> {
    check_testsuite(
        "https://github.com/oxigraph/oxigraph/tests/parser-unchecked/manifest.ttl",
        &[],
    )
    .await
}

#[tokio::test]
async fn oxigraph_parser_error_testsuite() -> Result<()> {
    check_testsuite(
        "https://github.com/oxigraph/oxigraph/tests/parser-error/manifest.ttl",
        &[],
    )
    .await
}

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
