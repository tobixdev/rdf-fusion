#![cfg(test)]

use anyhow::Result;
use rdf_fusion_testsuite::check_testsuite;

#[tokio::test]
async fn rdf_fusion_sparql_testsuite() -> Result<()> {
    check_testsuite(
        "https://github.com/tobixdev/rdf-fusion/blob/main/testsuite/rdf-fusion-tests/sparql/manifest.ttl",
        &[],
    )
    .await
}
