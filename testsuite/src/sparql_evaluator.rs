use crate::files::*;
use crate::manifest::*;
use crate::report::{dataset_diff, format_diff};
use crate::vocab::*;
use anyhow::{Context, Error, Result, bail, ensure};
use futures::StreamExt;
use rdf_fusion::io::RdfParser;
use rdf_fusion::model::dataset::CanonicalizationAlgorithm;
use rdf_fusion::model::vocab::*;
use rdf_fusion::model::*;
use rdf_fusion::store::Store;
use rdf_fusion::{Query, QueryOptions, QueryResults, Update};
use sparesults::QueryResultsFormat;
use std::collections::HashMap;
use std::fmt::Write;
use std::io::Cursor;
use std::str::FromStr;

pub fn sparql_evaluate_positive_syntax_test(test: &Test) -> Result<()> {
    let query_file = test.action.as_deref().context("No action found")?;
    let query = Query::parse(&read_file_to_string(query_file)?, Some(query_file))
        .context("Not able to parse")?;
    Query::parse(&query.to_string(), None)
        .with_context(|| format!("Failure to deserialize \"{query}\""))?;
    Ok(())
}

pub fn sparql_evaluate_negative_syntax_test(test: &Test) -> Result<()> {
    let query_file = test.action.as_deref().context("No action found")?;
    ensure!(
        Query::parse(&read_file_to_string(query_file)?, Some(query_file)).is_err(),
        "Oxigraph parses even if it should not."
    );
    Ok(())
}

pub async fn sparql_evaluate_positive_result_syntax_test(
    test: &Test,
    format: QueryResultsFormat,
) -> Result<()> {
    let action_file = test.action.as_deref().context("No action found")?;
    let actual_results = StaticQueryResults::from_query_results(
        QueryResults::read(read_file(action_file)?, format)?,
        true,
    )
    .await?;
    if let Some(result_file) = test.result.as_deref() {
        let expected_results = StaticQueryResults::from_query_results(
            QueryResults::read(read_file(result_file)?, format)?,
            true,
        )
        .await?;
        ensure!(
            are_query_results_isomorphic(&expected_results, &actual_results),
            "Not isomorphic results:\n{}\n",
            results_diff(expected_results, actual_results),
        );
    }
    Ok(())
}

pub async fn sparql_evaluate_negative_result_syntax_test(
    test: &Test,
    format: QueryResultsFormat,
) -> Result<()> {
    let action_file = test.action.as_deref().context("No action found")?;
    let query_results =
        QueryResults::read(Cursor::new(read_file_to_string(action_file)?), format)
            .map_err(Error::from);
    ensure!(
        query_results.is_err(),
        "Oxigraph parses even if it should not."
    );
    Ok(())
}

pub async fn sparql_evaluate_evaluation_test(test: &Test) -> Result<()> {
    let store = Store::new();
    if let Some(data) = &test.data {
        load_to_store(data, &store, GraphName::DefaultGraph).await?;
    }
    for (name, value) in &test.graph_data {
        load_to_store(value, &store, name.clone()).await?;
    }
    store.validate().await?;

    let query_file = test.query.as_deref().context("No action found")?;
    let options = QueryOptions::default();
    let query = Query::parse(&read_file_to_string(query_file)?, Some(query_file))
        .context("Failure to parse query")?;

    // We check parsing roundtrip
    Query::parse(&query.to_string(), None)
        .with_context(|| format!("Failure to deserialize \"{query}\""))?;

    // FROM and FROM NAMED support. We make sure the data is in the store
    if !query.dataset().is_default_dataset() {
        for graph_name in query.dataset().default_graph_graphs().unwrap_or(&[]) {
            let GraphName::NamedNode(graph_name) = graph_name else {
                bail!("Invalid FROM in query {query}");
            };
            load_to_store(graph_name.as_str(), &store, graph_name.as_ref()).await?;
        }
        for graph_name in query.dataset().available_named_graphs().unwrap_or(&[]) {
            let NamedOrBlankNode::NamedNode(graph_name) = graph_name else {
                bail!("Invalid FROM NAMED in query {query}");
            };
            load_to_store(graph_name.as_str(), &store, graph_name.as_ref()).await?;
        }
    }

    let expected_results = load_sparql_query_result(test.result.as_ref().unwrap())
        .await
        .context("Error constructing expected graph")?;
    let with_order =
        if let StaticQueryResults::Solutions { ordered, .. } = &expected_results {
            *ordered
        } else {
            false
        };

    let options = options.clone();
    let actual_results = store
        .query_opt(query.clone(), options)
        .await
        .context("Failure to execute query")?;
    let actual_results =
        StaticQueryResults::from_query_results(actual_results, with_order).await?;

    ensure!(
        are_query_results_isomorphic(&expected_results, &actual_results),
        "Not isomorphic results.\n{}\nParsed query:\n{}\nData:\n{:?}\n",
        results_diff(expected_results, actual_results),
        Query::parse(&read_file_to_string(query_file)?, Some(query_file))?,
        store.stream().await?.try_collect_to_vec().await?
    );
    Ok(())
}

pub fn sparql_evaluate_positive_update_syntax_test(test: &Test) -> Result<()> {
    let update_file = test.action.as_deref().context("No action found")?;
    let update = Update::parse(&read_file_to_string(update_file)?, Some(update_file))
        .context("Not able to parse")?;
    Update::parse(&update.to_string(), None)
        .with_context(|| format!("Failure to deserialize \"{update}\""))?;
    Ok(())
}

pub fn sparql_evaluate_negative_update_syntax_test(test: &Test) -> Result<()> {
    let update_file = test.action.as_deref().context("No action found")?;
    ensure!(
        Update::parse(&read_file_to_string(update_file)?, Some(update_file)).is_err(),
        "Oxigraph parses even if it should not."
    );
    Ok(())
}

pub async fn sparql_evaluate_update_evaluation_test(test: &Test) -> Result<()> {
    let store = Store::new();
    if let Some(data) = &test.data {
        load_to_store(data, &store, GraphName::DefaultGraph).await?;
    }
    for (name, value) in &test.graph_data {
        load_to_store(value, &store, name.clone()).await?;
    }

    let result_store = Store::new();
    if let Some(data) = &test.result {
        load_to_store(data, &result_store, GraphName::DefaultGraph).await?;
    }
    for (name, value) in &test.result_graph_data {
        load_to_store(value, &result_store, name.clone()).await?;
    }

    let update_file = test.update.as_deref().context("No action found")?;
    let update = Update::parse(&read_file_to_string(update_file)?, Some(update_file))
        .context("Failure to parse update")?;

    // We check parsing roundtrip
    Update::parse(&update.to_string(), None)
        .with_context(|| format!("Failure to deserialize \"{update}\""))?;

    store
        .update(update)
        .await
        .context("Failure to execute update")?;
    let mut store_dataset: Dataset = store
        .stream()
        .await?
        .try_collect_to_vec()
        .await?
        .into_iter()
        .collect();
    store_dataset.canonicalize(CanonicalizationAlgorithm::Unstable);
    let mut result_store_dataset: Dataset = result_store
        .stream()
        .await?
        .try_collect_to_vec()
        .await?
        .into_iter()
        .collect();
    result_store_dataset.canonicalize(CanonicalizationAlgorithm::Unstable);
    ensure!(
        store_dataset == result_store_dataset,
        "Not isomorphic result dataset.\nDiff:\n{}\nParsed update:\n{}\n",
        dataset_diff(&result_store_dataset, &store_dataset),
        Update::parse(&read_file_to_string(update_file)?, Some(update_file)).unwrap(),
    );
    Ok(())
}

async fn load_sparql_query_result(url: &str) -> Result<StaticQueryResults> {
    if let Some(format) = url
        .rsplit_once('.')
        .and_then(|(_, extension)| QueryResultsFormat::from_extension(extension))
    {
        StaticQueryResults::from_query_results(
            QueryResults::read(read_file(url)?, format)?,
            false,
        )
        .await
    } else {
        StaticQueryResults::from_graph(&load_graph(url, guess_rdf_format(url)?, false)?)
            .await
    }
}

async fn to_graph(result: QueryResults, with_order: bool) -> Result<Graph> {
    Ok(match result {
        QueryResults::Graph(mut graph) => graph.collect_as_graph().await?,
        QueryResults::Boolean(value) => {
            let mut graph = Graph::new();
            let result_set = BlankNode::default();
            graph.insert(TripleRef::new(&result_set, rdf::TYPE, rs::RESULT_SET));
            graph.insert(TripleRef::new(
                &result_set,
                rs::BOOLEAN,
                &Literal::from(value),
            ));
            graph
        }
        QueryResults::Solutions(mut solutions) => {
            let mut graph = Graph::new();
            let result_set = BlankNode::default();
            graph.insert(TripleRef::new(&result_set, rdf::TYPE, rs::RESULT_SET));
            for variable in solutions.variables() {
                graph.insert(TripleRef::new(
                    &result_set,
                    rs::RESULT_VARIABLE,
                    LiteralRef::new_simple_literal(variable.as_str()),
                ));
            }
            let mut i = 0;
            while let Some(solution) = solutions.next().await {
                let solution = solution?;
                let solution_id = BlankNode::default();
                graph.insert(TripleRef::new(&result_set, rs::SOLUTION, &solution_id));
                for (variable, value) in solution.iter() {
                    let binding = BlankNode::default();
                    graph.insert(TripleRef::new(&solution_id, rs::BINDING, &binding));
                    graph.insert(TripleRef::new(&binding, rs::VALUE, value));
                    graph.insert(TripleRef::new(
                        &binding,
                        rs::VARIABLE,
                        LiteralRef::new_simple_literal(variable.as_str()),
                    ));
                }
                if with_order {
                    graph.insert(TripleRef::new(
                        &solution_id,
                        rs::INDEX,
                        &Literal::from(i128::from(i + 1)),
                    ));
                }
                i += 1;
            }
            graph
        }
    })
}

fn are_query_results_isomorphic(
    expected: &StaticQueryResults,
    actual: &StaticQueryResults,
) -> bool {
    match (expected, actual) {
        (
            StaticQueryResults::Solutions {
                variables: expected_variables,
                solutions: expected_solutions,
                ordered,
            },
            StaticQueryResults::Solutions {
                variables: actual_variables,
                solutions: actual_solutions,
                ..
            },
        ) => {
            expected_variables == actual_variables
                && expected_solutions.len() == actual_solutions.len()
                && if *ordered {
                    expected_solutions.iter().zip(actual_solutions).all(
                        |(expected_solution, actual_solution)| {
                            compare_solutions(expected_solution, actual_solution)
                        },
                    )
                } else {
                    expected_solutions.iter().all(|expected_solution| {
                        actual_solutions.iter().any(|actual_solution| {
                            compare_solutions(expected_solution, actual_solution)
                        })
                    })
                }
        }
        (StaticQueryResults::Boolean(expected), StaticQueryResults::Boolean(actual)) => {
            expected == actual
        }
        (StaticQueryResults::Graph(expected), StaticQueryResults::Graph(actual)) => {
            expected == actual
        }
        _ => false,
    }
}

fn compare_solutions(expected: &[(Variable, Term)], actual: &[(Variable, Term)]) -> bool {
    let mut bnode_map = HashMap::new();
    expected.len() == actual.len()
        && expected.iter().zip(actual).all(
            move |(
                (expected_variable, expected_value),
                (actual_variable, actual_value),
            )| {
                expected_variable == actual_variable
                    && compare_terms(
                        expected_value.as_ref(),
                        actual_value.as_ref(),
                        &mut bnode_map,
                    )
            },
        )
}

fn compare_terms<'a>(
    expected: TermRef<'a>,
    actual: TermRef<'a>,
    bnode_map: &mut HashMap<BlankNodeRef<'a>, BlankNodeRef<'a>>,
) -> bool {
    match (expected, actual) {
        (TermRef::BlankNode(expected), TermRef::BlankNode(actual)) => {
            expected == *bnode_map.entry(actual).or_insert(expected)
        }
        (expected, actual) => {
            if expected == actual {
                return true;
            }

            let value_lhs = TypedValueRef::try_from(expected);
            let value_rhs = TypedValueRef::try_from(actual);
            if let (Ok(value_lhs), Ok(value_rhs)) = (value_lhs, value_rhs) {
                value_lhs == value_rhs
            } else {
                // If these are ill-formed literals, they must be the same term to match
                // TODO: Check if this is standard conform
                false
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum StaticQueryResults {
    Graph(Graph),
    Solutions {
        variables: Vec<Variable>,
        solutions: Vec<Vec<(Variable, Term)>>,
        ordered: bool,
    },
    Boolean(bool),
}

impl StaticQueryResults {
    async fn from_query_results(results: QueryResults, with_order: bool) -> Result<Self> {
        Self::from_graph(&to_graph(results, with_order).await?).await
    }

    async fn from_graph(graph: &Graph) -> Result<Self> {
        // Hack to normalize literals
        let store = Store::new();
        for t in graph {
            store.insert(t.in_graph(GraphNameRef::DefaultGraph)).await?;
        }
        let mut graph = store
            .stream()
            .await?
            .try_collect_to_vec()
            .await?
            .into_iter()
            .map(|q| Ok(Triple::from(q)))
            .collect::<Result<Graph>>()?;

        if let Some(result_set) =
            graph.subject_for_predicate_object(rdf::TYPE, rs::RESULT_SET)
        {
            if let Some(bool) =
                graph.object_for_subject_predicate(result_set, rs::BOOLEAN)
            {
                // Boolean query
                Ok(Self::Boolean(bool == Literal::from(true).as_ref().into()))
            } else {
                // Regular query
                let mut variables: Vec<Variable> = graph
                    .objects_for_subject_predicate(result_set, rs::RESULT_VARIABLE)
                    .map(|object| {
                        let TermRef::Literal(l) = object else {
                            bail!("Invalid rs:resultVariable: {object}")
                        };
                        Ok(Variable::new_unchecked(l.value()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                variables.sort();

                let mut solutions = graph
                    .objects_for_subject_predicate(result_set, rs::SOLUTION)
                    .map(|object| {
                        let TermRef::BlankNode(solution) = object else {
                            bail!("Invalid rs:solution: {object}")
                        };
                        let mut bindings = graph
                            .objects_for_subject_predicate(solution, rs::BINDING)
                            .map(|object| {
                                let TermRef::BlankNode(binding) = object else {
                                    bail!("Invalid rs:binding: {object}")
                                };
                                let (Some(TermRef::Literal(variable)), Some(value)) = (
                                    graph.object_for_subject_predicate(
                                        binding,
                                        rs::VARIABLE,
                                    ),
                                    graph
                                        .object_for_subject_predicate(binding, rs::VALUE),
                                ) else {
                                    bail!("Invalid rs:binding: {binding}")
                                };
                                Ok((
                                    Variable::new_unchecked(variable.value()),
                                    value.into_owned(),
                                ))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        bindings.sort_by(|(a, _), (b, _)| a.cmp(b));
                        let index = graph
                            .object_for_subject_predicate(solution, rs::INDEX)
                            .map(|object| {
                                let TermRef::Literal(l) = object else {
                                    bail!("Invalid rs:index: {object}")
                                };
                                Ok(u64::from_str(l.value())?)
                            })
                            .transpose()?;
                        Ok((bindings, index))
                    })
                    .collect::<Result<Vec<_>>>()?;
                solutions.sort_by(|(_, index_a), (_, index_b)| index_a.cmp(index_b));

                let ordered = solutions.iter().all(|(_, index)| index.is_some());

                Ok(Self::Solutions {
                    variables,
                    solutions: solutions
                        .into_iter()
                        .map(|(solution, _)| solution)
                        .collect(),
                    ordered,
                })
            }
        } else {
            graph.canonicalize(CanonicalizationAlgorithm::Unstable);
            Ok(Self::Graph(graph))
        }
    }
}

fn results_diff(expected: StaticQueryResults, actual: StaticQueryResults) -> String {
    match expected {
        StaticQueryResults::Solutions {
            variables: mut expected_variables,
            solutions: expected_solutions,
            ordered,
        } => match actual {
            StaticQueryResults::Solutions {
                variables: mut actual_variables,
                solutions: actual_solutions,
                ..
            } => {
                let mut out = String::new();
                expected_variables.sort_unstable();
                actual_variables.sort_unstable();
                if expected_variables != actual_variables {
                    write!(
                        &mut out,
                        "Variables diff:\n{}",
                        format_diff(
                            &expected_variables
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join("\n"),
                            &actual_variables
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join("\n"),
                            "variables",
                        )
                    )
                    .unwrap();
                }
                write!(
                    &mut out,
                    "Solutions diff:\n{}",
                    format_diff(
                        &solutions_to_string(expected_solutions, ordered),
                        &solutions_to_string(actual_solutions, ordered),
                        "solutions",
                    )
                )
                .unwrap();
                out
            }
            StaticQueryResults::Boolean(actual) => {
                format!("Expecting solutions but found the boolean {actual}")
            }
            StaticQueryResults::Graph(actual) => {
                format!("Expecting solutions but found the graph:\n{actual}")
            }
        },
        StaticQueryResults::Graph(expected) => match actual {
            StaticQueryResults::Solutions { .. } => {
                "Expecting a graph but found solutions".into()
            }
            StaticQueryResults::Boolean(actual) => {
                format!("Expecting a graph but found the boolean {actual}")
            }
            StaticQueryResults::Graph(actual) => {
                let expected = expected
                    .into_iter()
                    .map(|t| t.in_graph(GraphNameRef::DefaultGraph))
                    .collect();
                let actual = actual
                    .into_iter()
                    .map(|t| t.in_graph(GraphNameRef::DefaultGraph))
                    .collect();
                dataset_diff(&expected, &actual)
            }
        },
        StaticQueryResults::Boolean(expected) => match actual {
            StaticQueryResults::Solutions { .. } => {
                "Expecting a boolean but found solutions".into()
            }
            StaticQueryResults::Boolean(actual) => {
                format!("Expecting {expected} but found {actual}")
            }
            StaticQueryResults::Graph(actual) => {
                format!("Expecting solutions but found the graph:\n{actual}")
            }
        },
    }
}

fn solutions_to_string(solutions: Vec<Vec<(Variable, Term)>>, ordered: bool) -> String {
    let mut lines = solutions
        .into_iter()
        .map(|mut s| {
            let mut out = String::new();
            out.write_str("{").unwrap();
            s.sort_unstable_by(|(v1, _), (v2, _)| v1.cmp(v2));
            for (variable, value) in s {
                write!(&mut out, "{variable} = {value} ").unwrap();
            }
            out.write_str("}").unwrap();
            out
        })
        .collect::<Vec<_>>();
    if !ordered {
        lines.sort_unstable();
    }
    lines.join("\n")
}

async fn load_to_store(
    url: &str,
    store: &Store,
    to_graph_name: impl Into<GraphName>,
) -> Result<()> {
    store
        .load_from_reader(
            RdfParser::from_format(guess_rdf_format(url)?)
                .with_base_iri(url)?
                .with_default_graph(to_graph_name),
            read_file(url)?,
        )
        .await?;
    Ok(())
}
