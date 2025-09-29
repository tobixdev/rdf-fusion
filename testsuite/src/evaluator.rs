use crate::manifest::Test;
use crate::report::TestResult;
use crate::sparql_evaluator::{
    sparql_evaluate_evaluation_test, sparql_evaluate_negative_result_syntax_test,
    sparql_evaluate_negative_syntax_test, sparql_evaluate_negative_update_syntax_test,
    sparql_evaluate_positive_result_syntax_test, sparql_evaluate_positive_syntax_test,
    sparql_evaluate_positive_update_syntax_test, sparql_evaluate_update_evaluation_test,
};
use anyhow::{Result, anyhow};
use datafusion::common::runtime::SpawnedTask;
use sparesults::QueryResultsFormat;
use time::OffsetDateTime;

#[derive(Debug, Default)]
pub struct TestEvaluator;

impl TestEvaluator {
    pub async fn evaluate(
        &self,
        manifest: impl Iterator<Item = Result<Test>> + Send + 'static,
    ) -> Result<Vec<TestResult>> {
        let mut results = Vec::new();

        for test in manifest {
            let test = test?;
            let test_id = test.id.clone();
            let outcome =
                SpawnedTask::spawn(handle_test(test))
                    .await
                    .unwrap_or_else(|err| {
                        Err(anyhow!("Could not join on test tasks. {err}"))
                    });
            results.push(TestResult {
                test: test_id,
                outcome,
                date: OffsetDateTime::now_utc(),
            });
        }

        Ok(results)
    }
}

async fn handle_test(test: Test) -> Result<()> {
    match test.kind.as_str() {
        // == SPARQL Tests ==
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest"
        | "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest11" => {
            sparql_evaluate_positive_syntax_test(&test)
        }
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeSyntaxTest"
        | "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeSyntaxTest11" => {
            sparql_evaluate_negative_syntax_test(&test)
        }
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#QueryEvaluationTest" => {
            sparql_evaluate_evaluation_test(&test).await
        }
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveUpdateSyntaxTest11" => {
            sparql_evaluate_positive_update_syntax_test(&test)
        }
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeUpdateSyntaxTest11" => {
            sparql_evaluate_negative_update_syntax_test(&test)
        }
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#UpdateEvaluationTest" => {
            sparql_evaluate_update_evaluation_test(&test).await
        }

        // == Oxigraph Tests ==
        "https://github.com/oxigraph/oxigraph/tests#PositiveJsonResultsSyntaxTest" => {
            sparql_evaluate_positive_result_syntax_test(&test, QueryResultsFormat::Json)
                .await
        }
        "https://github.com/oxigraph/oxigraph/tests#NegativeJsonResultsSyntaxTest" => {
            sparql_evaluate_negative_result_syntax_test(&test, QueryResultsFormat::Json)
                .await
        }
        "https://github.com/oxigraph/oxigraph/tests#PositiveXmlResultsSyntaxTest" => {
            sparql_evaluate_positive_result_syntax_test(&test, QueryResultsFormat::Xml)
                .await
        }
        "https://github.com/oxigraph/oxigraph/tests#NegativeXmlResultsSyntaxTest" => {
            sparql_evaluate_negative_result_syntax_test(&test, QueryResultsFormat::Xml)
                .await
        }
        "https://github.com/oxigraph/oxigraph/tests#NegativeTsvResultsSyntaxTest" => {
            sparql_evaluate_negative_result_syntax_test(&test, QueryResultsFormat::Tsv)
                .await
        }
        _ => Err(anyhow!("The test type {} is not supported", test.kind)),
    }
}
