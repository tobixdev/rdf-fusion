use crate::evaluator::TestEvaluator;
use crate::files::{guess_rdf_format, load_dataset, load_n3, read_file_to_string};
use crate::manifest::Test;
use crate::report::{dataset_diff, format_diff};
use anyhow::{bail, ensure, Context, Result};
use oxigraph::io::RdfFormat;
use oxrdf::dataset::CanonicalizationAlgorithm;
use oxrdf::{BlankNode, Dataset, Quad};
use oxttl::n3::{N3Quad, N3Term};
use tokio::runtime::Runtime;

pub fn register_parser_tests(evaluator: &mut TestEvaluator) {
    evaluator.register(
        "http://www.w3.org/ns/rdftest#TestNTriplesPositiveSyntax",
        |r, t| evaluate_positive_syntax_test(r, t, RdfFormat::NTriples),
    );
    evaluator.register(
        "http://www.w3.org/ns/rdftest#TestNQuadsPositiveSyntax",
        |r, t| evaluate_positive_syntax_test(r, t, RdfFormat::NQuads),
    );
    evaluator.register(
        "http://www.w3.org/ns/rdftest#TestTurtlePositiveSyntax",
        |r, t| evaluate_positive_syntax_test(r, t, RdfFormat::Turtle),
    );
    evaluator.register(
        "http://www.w3.org/ns/rdftest#TestTrigPositiveSyntax",
        |r, t| evaluate_positive_syntax_test(r, t, RdfFormat::TriG),
    );
    evaluator.register(
        "https://w3c.github.io/N3/tests/test.n3#TestN3PositiveSyntax",
        evaluate_positive_n3_syntax_test,
    );
    evaluator.register(
        "http://www.w3.org/ns/rdftest#TestNTriplesNegativeSyntax",
        |r, t| evaluate_negative_syntax_test(r, t, RdfFormat::NTriples),
    );
    evaluator.register(
        "http://www.w3.org/ns/rdftest#TestNQuadsNegativeSyntax",
        |r, t| evaluate_negative_syntax_test(r, t, RdfFormat::NQuads),
    );
    evaluator.register(
        "http://www.w3.org/ns/rdftest#TestTurtleNegativeSyntax",
        |r, t| evaluate_negative_syntax_test(r, t, RdfFormat::Turtle),
    );
    evaluator.register(
        "http://www.w3.org/ns/rdftest#TestTrigNegativeSyntax",
        |r, t| evaluate_negative_syntax_test(r, t, RdfFormat::TriG),
    );
    evaluator.register(
        "http://www.w3.org/ns/rdftest#TestXMLNegativeSyntax",
        |r, t| evaluate_negative_syntax_test(r, t, RdfFormat::RdfXml),
    );
    evaluator.register(
        "https://w3c.github.io/N3/tests/test.n3#TestN3NegativeSyntax",
        evaluate_negative_n3_syntax_test,
    );
    evaluator.register("http://www.w3.org/ns/rdftest#TestTurtleEval", |r, t| {
        evaluate_eval_test(r, t, RdfFormat::Turtle, false, false)
    });
    evaluator.register("http://www.w3.org/ns/rdftest#TestTrigEval", |r, t| {
        evaluate_eval_test(r, t, RdfFormat::TriG, false, false)
    });
    evaluator.register("http://www.w3.org/ns/rdftest#TestXMLEval", |r, t| {
        evaluate_eval_test(r, t, RdfFormat::RdfXml, false, false)
    });
    evaluator.register(
        "https://w3c.github.io/N3/tests/test.n3#TestN3Eval",
        |r, t| evaluate_n3_eval_test(r, t, false),
    );
    evaluator.register(
        "http://www.w3.org/ns/rdftest#TestTurtleNegativeEval",
        |r, t| evaluate_negative_syntax_test(r, t, RdfFormat::Turtle),
    );
    evaluator.register(
        "http://www.w3.org/ns/rdftest#TestTrigNegativeEval",
        |r, t| evaluate_negative_syntax_test(r, t, RdfFormat::TriG),
    );
    evaluator.register(
        "http://www.w3.org/ns/rdftest#TestNTriplesPositiveC14N",
        |r, t| evaluate_positive_c14n_test(r, t, RdfFormat::NTriples),
    );
    evaluator.register(
        "https://w3c.github.io/rdf-canon/tests/vocab#RDFC10EvalTest",
        |r, t| evaluate_positive_syntax_test(r, t, RdfFormat::NQuads), //TODO: not a proper implementation!
    );
    evaluator.register(
        "https://w3c.github.io/rdf-canon/tests/vocab#RDFC10NegativeEvalTest",
        |_, _| Ok(()), // TODO: not a proper implementation
    );
    evaluator.register(
        "https://w3c.github.io/rdf-canon/tests/vocab#RDFC10MapTest",
        |_, _| Ok(()), // TODO: not a proper implementation
    );
    evaluator.register(
        "https://github.com/oxigraph/oxigraph/tests#TestNTripleRecovery",
        |r, t| evaluate_eval_test(r, t, RdfFormat::NTriples, true, false),
    );
    evaluator.register(
        "https://github.com/oxigraph/oxigraph/tests#TestNQuadRecovery",
        |r, t| evaluate_eval_test(r, t, RdfFormat::NQuads, true, false),
    );
    evaluator.register(
        "https://github.com/oxigraph/oxigraph/tests#TestTurtleRecovery",
        |r, t| evaluate_eval_test(r, t, RdfFormat::Turtle, true, false),
    );
    evaluator.register(
        "https://github.com/oxigraph/oxigraph/tests#TestN3Recovery",
        |r, t| evaluate_n3_eval_test(r, t, true),
    );
    evaluator.register(
        "https://github.com/oxigraph/oxigraph/tests#TestUncheckedTurtle",
        |r, t| evaluate_eval_test(r, t, RdfFormat::Turtle, true, true),
    );
}

fn evaluate_positive_syntax_test(_: &Runtime, test: &Test, format: RdfFormat) -> Result<()> {
    let action = test.action.as_deref().context("No action found")?;
    load_dataset(action, format, false, false).context("Parse error")?;
    Ok(())
}

fn evaluate_positive_n3_syntax_test(_: &Runtime, test: &Test) -> Result<()> {
    let action = test.action.as_deref().context("No action found")?;
    load_n3(action, false).context("Parse error")?;
    Ok(())
}

fn evaluate_negative_syntax_test(runtime: &Runtime, test: &Test, format: RdfFormat) -> Result<()> {
    runtime.block_on(async {
        let action = test.action.as_deref().context("No action found")?;
        let Err(error) = load_dataset(action, format, false, false) else {
            bail!("File parsed without errors even if it should not");
        };
        if let Some(result) = &test.result {
            let expected = read_file_to_string(result)?;
            ensure!(
                expected == error.to_string(),
                "Not expected error message:\n{}",
                format_diff(&expected, &error.to_string(), "message")
            );
        }
        Ok(())
    })
}

fn evaluate_negative_n3_syntax_test(runtime: &Runtime, test: &Test) -> Result<()> {
    runtime.block_on(async {
        let action = test.action.as_deref().context("No action found")?;
        ensure!(
            load_n3(action, false).is_err(),
            "File parsed without errors even if it should not"
        );
        Ok(())
    })
}

fn evaluate_eval_test(
    runtime: &Runtime,
    test: &Test,
    format: RdfFormat,
    ignore_errors: bool,
    lenient: bool,
) -> Result<()> {
    runtime.block_on(async {
        let action = test.action.as_deref().context("No action found")?;
        let mut actual_dataset = load_dataset(action, format, ignore_errors, lenient)
            .with_context(|| format!("Parse error on file {action}"))?;
        actual_dataset.canonicalize(CanonicalizationAlgorithm::Unstable);
        let results = test.result.as_ref().context("No tests result found")?;
        let mut expected_dataset =
            load_dataset(results, guess_rdf_format(results)?, false, lenient)
                .with_context(|| format!("Parse error on file {results}"))?;
        expected_dataset.canonicalize(CanonicalizationAlgorithm::Unstable);
        ensure!(
            expected_dataset == actual_dataset,
            "The two files are not isomorphic. Diff:\n{}",
            dataset_diff(&expected_dataset, &actual_dataset)
        );
        Ok(())
    })
}

fn evaluate_n3_eval_test(runtime: &Runtime, test: &Test, ignore_errors: bool) -> Result<()> {
    runtime.block_on(async {
        let action = test.action.as_deref().context("No action found")?;
        let mut actual_dataset = n3_to_dataset(
            load_n3(action, ignore_errors)
                .with_context(|| format!("Parse error on file {action}"))?,
        );
        actual_dataset.canonicalize(CanonicalizationAlgorithm::Unstable);
        let results = test.result.as_ref().context("No tests result found")?;
        let mut expected_dataset = n3_to_dataset(
            load_n3(results, false).with_context(|| format!("Parse error on file {results}"))?,
        );
        expected_dataset.canonicalize(CanonicalizationAlgorithm::Unstable);
        ensure!(
            expected_dataset == actual_dataset,
            "The two files are not isomorphic. Diff:\n{}",
            dataset_diff(&expected_dataset, &actual_dataset)
        );
        Ok(())
    })
}

fn evaluate_positive_c14n_test(runtime: &Runtime, test: &Test, format: RdfFormat) -> Result<()> {
    runtime.block_on(async {
        let action = test.action.as_deref().context("No action found")?;
        let actual = load_dataset(action, format, false, false)
            .with_context(|| format!("Parse error on file {action}"))?
            .to_string();
        let results = test.result.as_ref().context("No tests result found")?;
        let expected = read_file_to_string(results)
            .with_context(|| format!("Read error on file {results}"))?;
        ensure!(
            expected == actual,
            "The two files are not equal. Diff:\n{}",
            format_diff(&expected, &actual, "c14n")
        );
        Ok(())
    })
}

fn n3_to_dataset(quads: Vec<N3Quad>) -> Dataset {
    quads
        .into_iter()
        .filter_map(|q| {
            Some(Quad {
                subject: match q.subject {
                    N3Term::NamedNode(n) => n.into(),
                    N3Term::BlankNode(n) => n.into(),
                    N3Term::Triple(n) => n.into(),
                    N3Term::Literal(_) => return None,
                    N3Term::Variable(v) => BlankNode::new_unchecked(v.into_string()).into(),
                },
                predicate: match q.predicate {
                    N3Term::NamedNode(n) => n,
                    _ => return None,
                },
                object: match q.object {
                    N3Term::NamedNode(n) => n.into(),
                    N3Term::BlankNode(n) => n.into(),
                    N3Term::Triple(n) => n.into(),
                    N3Term::Literal(n) => n.into(),
                    N3Term::Variable(v) => BlankNode::new_unchecked(v.into_string()).into(),
                },
                graph_name: q.graph_name,
            })
        })
        .collect()
}
