use crate::manifest::Test;
use crate::parser_evaluator::{parser_evaluate_eval_test, parser_evaluate_n3_eval_test, parser_evaluate_negative_n3_syntax_test, parser_evaluate_negative_syntax_test, parser_evaluate_positive_c14n_test, parser_evaluate_positive_n3_syntax_test, parser_evaluate_positive_syntax_test};
use crate::report::TestResult;
use crate::sparql_evaluator::{
    sparql_evaluate_evaluation_test, sparql_evaluate_negative_result_syntax_test,
    sparql_evaluate_negative_syntax_test, sparql_evaluate_negative_update_syntax_test,
    sparql_evaluate_positive_result_syntax_test, sparql_evaluate_positive_syntax_test,
    sparql_evaluate_positive_update_syntax_test, sparql_evaluate_update_evaluation_test,
};
use anyhow::{anyhow, Result};
use graphfusion::io::RdfFormat;
use sparesults::QueryResultsFormat;
use time::OffsetDateTime;

pub struct TestEvaluator {}

impl TestEvaluator {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn evaluate(
        &self,
        manifest: impl Iterator<Item = Result<Test>>,
    ) -> Result<Vec<TestResult>> {
        let mut results = Vec::new();

        for test in manifest {
            let test = test?;
            let outcome = handle(&test).await;
            results.push(TestResult {
                test: test.id,
                outcome,
                date: OffsetDateTime::now_utc(),
            });
        }

        Ok(results)
    }
}

async fn handle(test: &Test) -> Result<()> {
    match test.kind.as_str() {
        // == Parser Tests ==
        "http://www.w3.org/ns/rdftest#TestNTriplesPositiveSyntax" => {
            parser_evaluate_positive_syntax_test(test, RdfFormat::NTriples)
        }
        "http://www.w3.org/ns/rdftest#TestNQuadsPositiveSyntax" => {
            parser_evaluate_positive_syntax_test(test, RdfFormat::NQuads)
        }
        "http://www.w3.org/ns/rdftest#TestTurtlePositiveSyntax" => {
            parser_evaluate_positive_syntax_test(test, RdfFormat::Turtle)
        }
        "http://www.w3.org/ns/rdftest#TestTrigPositiveSyntax" => {
            parser_evaluate_positive_syntax_test(test, RdfFormat::TriG)
        }
        "https://w3c.github.io/N3/tests/test.n3#TestN3PositiveSyntax" => {
            parser_evaluate_positive_n3_syntax_test(test)
        }
        "http://www.w3.org/ns/rdftest#TestNTriplesNegativeSyntax" => {
            parser_evaluate_negative_syntax_test(test, RdfFormat::NTriples)
        }
        "http://www.w3.org/ns/rdftest#TestNQuadsNegativeSyntax" => {
            parser_evaluate_negative_syntax_test(test, RdfFormat::NQuads)
        }
        "http://www.w3.org/ns/rdftest#TestTurtleNegativeSyntax" => {
            parser_evaluate_negative_syntax_test(test, RdfFormat::Turtle)
        }
        "http://www.w3.org/ns/rdftest#TestTrigNegativeSyntax" => {
            parser_evaluate_negative_syntax_test(test, RdfFormat::TriG)
        }
        "http://www.w3.org/ns/rdftest#TestXMLNegativeSyntax" => {
            parser_evaluate_negative_syntax_test(test, RdfFormat::RdfXml)
        }
        "https://w3c.github.io/N3/tests/test.n3#TestN3NegativeSyntax" => {
            parser_evaluate_negative_n3_syntax_test(test)
        }
        "http://www.w3.org/ns/rdftest#TestTurtleEval" => {
            parser_evaluate_eval_test(test, RdfFormat::Turtle, false, false)
        }
        "http://www.w3.org/ns/rdftest#TestTrigEval" => {
            parser_evaluate_eval_test(test, RdfFormat::TriG, false, false)
        }
        "http://www.w3.org/ns/rdftest#TestXMLEval" => {
            parser_evaluate_eval_test(test, RdfFormat::RdfXml, false, false)
        }
        "https://w3c.github.io/N3/tests/test.n3#TestN3Eval" => {
            parser_evaluate_n3_eval_test(test, false)
        }
        "http://www.w3.org/ns/rdftest#TestTurtleNegativeEval" => {
            parser_evaluate_negative_syntax_test(test, RdfFormat::Turtle)
        }
        "http://www.w3.org/ns/rdftest#TestTrigNegativeEval" => {
            parser_evaluate_negative_syntax_test(test, RdfFormat::TriG)
        }
        "http://www.w3.org/ns/rdftest#TestNTriplesPositiveC14N" => {
            parser_evaluate_positive_c14n_test(test, RdfFormat::NTriples)
        }
        "https://w3c.github.io/rdf-canon/tests/vocab#RDFC10EvalTest" => {
            parser_evaluate_positive_syntax_test(test, RdfFormat::NQuads)
        } //TODO: not a proper implementation!
        "https://w3c.github.io/rdf-canon/tests/vocab#RDFC10NegativeEvalTest" => Ok(()), //TODO: not a proper implementation!
        "https://w3c.github.io/rdf-canon/tests/vocab#RDFC10MapTest" => Ok(()), //TODO: not a proper implementation!
        "https://github.com/oxigraph/oxigraph/tests#TestNTripleRecovery" => {
            parser_evaluate_eval_test(test, RdfFormat::NTriples, true, false)
        }
        "https://github.com/oxigraph/oxigraph/tests#TestNQuadRecovery" => {
            parser_evaluate_eval_test(test, RdfFormat::NQuads, true, false)
        }
        "https://github.com/oxigraph/oxigraph/tests#TestTurtleRecovery" => {
            parser_evaluate_eval_test(test, RdfFormat::Turtle, true, false)
        }
        "https://github.com/oxigraph/oxigraph/tests#TestN3Recovery" => {
            parser_evaluate_n3_eval_test(test, true)
        }
        "https://github.com/oxigraph/oxigraph/tests#TestUncheckedTurtle" => {
            parser_evaluate_eval_test(test, RdfFormat::Turtle, true, true)
        }

        // == SPARQL Tests ==
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest" => {
            sparql_evaluate_positive_syntax_test(test)
        }
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest11" => {
            sparql_evaluate_positive_syntax_test(test)
        }
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeSyntaxTest" => {
            sparql_evaluate_negative_syntax_test(test)
        }
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeSyntaxTest11" => {
            sparql_evaluate_negative_syntax_test(test)
        }
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#QueryEvaluationTest" => {
            sparql_evaluate_evaluation_test(test).await
        }
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveUpdateSyntaxTest11" => {
            sparql_evaluate_positive_update_syntax_test(test)
        }
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeUpdateSyntaxTest11" => {
            sparql_evaluate_negative_update_syntax_test(test)
        }
        "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#UpdateEvaluationTest" => {
            sparql_evaluate_update_evaluation_test(test).await
        }

        // == Oxigraph Tests ==
        "https://github.com/oxigraph/oxigraph/tests#PositiveJsonResultsSyntaxTest" => {
            sparql_evaluate_positive_result_syntax_test(test, QueryResultsFormat::Json).await
        }
        "https://github.com/oxigraph/oxigraph/tests#NegativeJsonResultsSyntaxTest" => {
            sparql_evaluate_negative_result_syntax_test(test, QueryResultsFormat::Json).await
        }
        "https://github.com/oxigraph/oxigraph/tests#PositiveXmlResultsSyntaxTest" => {
            sparql_evaluate_positive_result_syntax_test(test, QueryResultsFormat::Xml).await
        }
        "https://github.com/oxigraph/oxigraph/tests#NegativeXmlResultsSyntaxTest" => {
            sparql_evaluate_negative_result_syntax_test(test, QueryResultsFormat::Xml).await
        }
        "https://github.com/oxigraph/oxigraph/tests#NegativeTsvResultsSyntaxTest" => {
            sparql_evaluate_negative_result_syntax_test(test, QueryResultsFormat::Tsv).await
        }
        _ => Err(anyhow!("The test type {} is not supported", test.kind)),
    }
}
