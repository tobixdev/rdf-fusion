//! This file contains tests for an adapted version of the Wind Farm benchmark. The adaption have
//! been made such that the queries produce stable results. The results of the queries has been
//! compared to GraphDB 11 to validate their correctness. See [crate::bsbm] for a detailed
//! description.

use crate::query_results::run_select_query;
use insta::assert_snapshot;
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::windfarm::{NumTurbines, WindFarmBenchmark};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;

#[tokio::test]
pub async fn wind_farm_4_test_results() {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);
    let benchmark = WindFarmBenchmark::new(NumTurbines::N4);
    let benchmark_name = benchmark.name();
    let ctx = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = benchmark.prepare_store(&ctx).await.unwrap();

    assert_snapshot!(
        "Production Q1",
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-production-query1.sparql")
        )
        .await
    );
    assert_snapshot!(
        "Production Q2",
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-production-query2.sparql")
        )
        .await
    );
    assert_snapshot!(
        "Production Q3",
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-production-query3.sparql")
        )
        .await
    );
    assert_snapshot!(
        "Production Q4",
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-production-query4.sparql")
        )
        .await
    );

    assert_snapshot!(
        "Grouped Production Q1",
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-grouped-production-query1.sparql")
        )
        .await
    );
    assert_snapshot!(
        "Grouped Production Q2",
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-grouped-production-query2.sparql")
        )
        .await
    );
    assert_snapshot!(
        "Grouped Production Q3",
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-grouped-production-query3.sparql")
        )
        .await
    );
    assert_snapshot!(
        "Grouped Production Q4",
        run_select_query(
            &store,
            include_str!("./queries/wind-farm-grouped-production-query4.sparql")
        )
        .await
    );
}
