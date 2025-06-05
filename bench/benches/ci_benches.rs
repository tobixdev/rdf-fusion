use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};
use rdf_fusion_bench::benchmarks::{BenchmarkName, BsbmDatasetSize};
use rdf_fusion_bench::{execute_benchmark_operation, Operation};
use tokio::runtime::Builder;

fn bsbm_1000_100(c: &mut Criterion) {
    let benchmark = BenchmarkName::Bsbm {
        dataset_size: BsbmDatasetSize::N1000,
        max_query_count: Some(100),
    };
    run_benchmark(c, benchmark);
}

fn run_benchmark(c: &mut Criterion, benchmark: BenchmarkName) {
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        execute_benchmark_operation(Operation::Prepare, benchmark)
            .await
            .expect("Failed to prepare benchmark.")
    });

    c.bench_function(&benchmark.to_string(), |b| {
        b.to_async(&runtime).iter(|| async {
            execute_benchmark_operation(Operation::Execute, benchmark)
                .await
                .expect("Failed to execute benchmark");
        });
    });
}

criterion_group!(bsbm, bsbm_1000_100);
criterion_main!(bsbm);
