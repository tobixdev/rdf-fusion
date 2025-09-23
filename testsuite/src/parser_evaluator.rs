use crate::files::{guess_rdf_format, load_dataset, load_n3, read_file_to_string};
use crate::manifest::Test;
use crate::report::{dataset_diff, format_diff};
use anyhow::{Context, Result, bail, ensure};
use oxrdfio::RdfFormat;
use oxttl::n3::{N3Quad, N3Term};
use rdf_fusion::model::dataset::CanonicalizationAlgorithm;
use rdf_fusion::model::{BlankNode, Dataset, Quad};

pub fn parser_evaluate_positive_syntax_test(
    test: &Test,
    format: RdfFormat,
) -> Result<()> {
    let action = test.action.as_deref().context("No action found")?;
    load_dataset(action, format, false, false).context("Parse error")?;
    Ok(())
}

pub fn parser_evaluate_positive_n3_syntax_test(test: &Test) -> Result<()> {
    let action = test.action.as_deref().context("No action found")?;
    load_n3(action, false).context("Parse error")?;
    Ok(())
}

pub fn parser_evaluate_negative_syntax_test(
    test: &Test,
    format: RdfFormat,
) -> Result<()> {
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
}

pub fn parser_evaluate_negative_n3_syntax_test(test: &Test) -> Result<()> {
    let action = test.action.as_deref().context("No action found")?;
    ensure!(
        load_n3(action, false).is_err(),
        "File parsed without errors even if it should not"
    );
    Ok(())
}

pub fn parser_evaluate_eval_test(
    test: &Test,
    format: RdfFormat,
    ignore_errors: bool,
    lenient: bool,
) -> Result<()> {
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
}

pub fn parser_evaluate_n3_eval_test(test: &Test, ignore_errors: bool) -> Result<()> {
    let action = test.action.as_deref().context("No action found")?;
    let mut actual_dataset = n3_to_dataset(
        load_n3(action, ignore_errors)
            .with_context(|| format!("Parse error on file {action}"))?,
    );
    actual_dataset.canonicalize(CanonicalizationAlgorithm::Unstable);
    let results = test.result.as_ref().context("No tests result found")?;
    let mut expected_dataset = n3_to_dataset(
        load_n3(results, false)
            .with_context(|| format!("Parse error on file {results}"))?,
    );
    expected_dataset.canonicalize(CanonicalizationAlgorithm::Unstable);
    ensure!(
        expected_dataset == actual_dataset,
        "The two files are not isomorphic. Diff:\n{}",
        dataset_diff(&expected_dataset, &actual_dataset)
    );
    Ok(())
}

pub fn parser_evaluate_positive_c14n_test(test: &Test, format: RdfFormat) -> Result<()> {
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
}

fn n3_to_dataset(quads: Vec<N3Quad>) -> Dataset {
    quads
        .into_iter()
        .filter_map(|q| {
            Some(Quad {
                subject: match q.subject {
                    N3Term::NamedNode(n) => n.into(),
                    N3Term::BlankNode(n) => n.into(),
                    N3Term::Literal(_) => return None,
                    N3Term::Variable(v) => {
                        BlankNode::new_unchecked(v.into_string()).into()
                    }
                },
                predicate: match q.predicate {
                    N3Term::NamedNode(n) => n,
                    _ => return None,
                },
                object: match q.object {
                    N3Term::NamedNode(n) => n.into(),
                    N3Term::BlankNode(n) => n.into(),
                    N3Term::Literal(n) => n.into(),
                    N3Term::Variable(v) => {
                        BlankNode::new_unchecked(v.into_string()).into()
                    }
                },
                graph_name: q.graph_name,
            })
        })
        .collect()
}
