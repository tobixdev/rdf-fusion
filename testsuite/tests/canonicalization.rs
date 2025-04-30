#![cfg(test)]

use anyhow::Result;
use graphfusion_testsuite::check_testsuite;

#[tokio::test]
#[ignore = "Test type not implemented"]
async fn rdf_canon_w3c_testsuite() -> Result<()> {
    check_testsuite("https://w3c.github.io/rdf-canon/tests/manifest.ttl", &[]).await
}
