//! Implementation of [W3C RDF tests](https://w3c.github.io/rdf-tests/) to tests Oxigraph conformance.

pub mod evaluator;
pub mod files;
pub mod manifest;
pub mod parser_evaluator;
pub mod report;
pub mod sparql_evaluator;
mod vocab;

use crate::evaluator::TestEvaluator;
use crate::manifest::TestManifest;
use anyhow::Result;

#[allow(clippy::panic_in_result_fn)]
pub async fn check_testsuite(manifest_url: &str, ignored_tests: &[&str]) -> Result<()> {
    let evaluator = TestEvaluator::new();
    let manifest = TestManifest::new([manifest_url]);
    let results = evaluator.evaluate(manifest).await?;
    let test_count = results.len();

    let mut errors = Vec::default();
    for result in results {
        if let Err(error) = &result.outcome {
            if !ignored_tests.contains(&result.test.as_str()) {
                errors.push(format!("{}: failed with error {error:?}", result.test))
            }
        }
    }

    assert!(
        errors.is_empty(),
        "{} tests failing from {} tests:\n{}\n",
        errors.len(),
        test_count,
        errors.join("\n")
    );
    Ok(())
}
