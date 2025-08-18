//! This file contains tests for an adapte version of the BSBM explore use case. The adaption have
//! been made such that the queries produce stable results.
//!
//! The results have been compared against GraphDB 11.0.1. The following curl command can be used to
//! get the results of GraphDB. Ideally, one can pipe the result of the request into a file or
//! directly to the clipboard using `| xclip -selection clipboard` (or similar). You can also
//! download the JSON result from the GraphDB UI.
//!
//! ```bash
//! curl -X POST \
//!     "http://<graphdb_url>/repositories/bsbm" \
//!     -H "Content-Type: application/sparql-query" \
//!     -H "Accept: application/sparql-results+json" \
//!     --data-binary '<query>'
//! ```
//!
//! Then, even though we pretty-print our results, there will be some differences (e.g., spacing,
//! order of keys). You can use a tool that semantically compares JSON files to "quickly" check
//! the results of a new test (e.g., [JSON Compare](https://jsoncompare.org/)). `CONSTRUCT` queries
//! have been compared manually.

use futures::StreamExt;
use insta::assert_snapshot;
use rdf_fusion::io::{RdfFormat, RdfSerializer};
use rdf_fusion::results::{QueryResultsFormat, QueryResultsSerializer};
use rdf_fusion::store::Store;
use rdf_fusion::QueryResults;
use rdf_fusion_bench::benchmarks::bsbm::NumProducts::N1_000;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase};
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use serde_json::Value;
use std::path::PathBuf;

#[tokio::test]
pub async fn bsbm_1000_test_results() {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"));
    let benchmark = BsbmBenchmark::<ExploreUseCase>::try_new(N1_000, None).unwrap();
    let benchmark_name = benchmark.name();
    let ctx = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = benchmark.prepare_store(&ctx).await.unwrap();

    assert_snapshot!(
        "Explore Q1",
        run_select_query(&store, include_str!("./queries/explore-q1.sparql")).await
    );
    assert_snapshot!(
        "Explore Q2",
        run_select_query(&store, include_str!("./queries/explore-q2.sparql")).await
    );
    assert_snapshot!(
        "Explore Q3",
        run_select_query(&store, include_str!("./queries/explore-q3.sparql")).await
    );
    assert_snapshot!(
        "Explore Q5",
        run_select_query(&store, include_str!("./queries/explore-q5.sparql")).await
    );
    assert_snapshot!(
        "Explore Q7",
        run_select_query(&store, include_str!("./queries/explore-q7.sparql")).await
    );
    assert_snapshot!(
        "Explore Q8",
        run_select_query(&store, include_str!("./queries/explore-q8.sparql")).await
    );
    assert_snapshot!(
        "Explore Q10",
        run_select_query(&store, include_str!("./queries/explore-q10.sparql")).await
    );
    assert_snapshot!(
        "Explore Q11",
        run_select_query(&store, include_str!("./queries/explore-q11.sparql")).await
    );
    assert_snapshot!(
        "Explore Q12",
        run_construct_query(&store, include_str!("./queries/explore-q12.sparql")).await
    );
}

async fn run_select_query(store: &Store, query: &str) -> String {
    let result = store.query(query).await.unwrap();
    let QueryResults::Solutions(mut solutions) = result else {
        panic!("Unexpected result format!")
    };

    let mut buffer = Vec::new();
    let mut serializer = QueryResultsSerializer::from_format(QueryResultsFormat::Json)
        .serialize_solutions_to_writer(&mut buffer, solutions.variables().to_vec())
        .unwrap();
    while let Some(solution) = solutions.next().await {
        let solution = solution.unwrap();
        serializer.serialize(solution.iter()).unwrap();
    }
    serializer.finish().unwrap();

    let raw_json = String::from_utf8(buffer).unwrap();
    let v: Value = serde_json::from_str(&raw_json).unwrap();
    serde_json::to_string_pretty(&v).unwrap()
}

async fn run_construct_query(store: &Store, query: &str) -> String {
    let result = store.query(query).await.unwrap();
    let QueryResults::Graph(mut solutions) = result else {
        panic!("Unexpected result format!")
    };

    let mut buffer = Vec::new();
    let mut serializer =
        RdfSerializer::from_format(RdfFormat::Turtle).for_writer(&mut buffer);
    while let Some(solution) = solutions.next().await {
        let solution = solution.unwrap();
        serializer.serialize_triple(solution.as_ref()).unwrap();
    }
    serializer.finish().unwrap();

    String::from_utf8(buffer).unwrap()
}
