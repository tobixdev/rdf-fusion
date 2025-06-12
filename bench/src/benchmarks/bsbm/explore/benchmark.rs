use crate::benchmarks::bsbm::explore::operation::{
    list_raw_operations, BsbmExplorOperation, BsbmExploreRawOperation,
};
use crate::benchmarks::bsbm::explore::report::{ExploreReport, ExploreReportBuilder};
use crate::benchmarks::bsbm::BsbmDatasetSize;
use crate::benchmarks::{Benchmark, BenchmarkName};
use crate::environment::{BenchmarkContext, RdfFusionBenchContext};
use crate::prepare::{ArchiveType, FileDownloadAction, PrepRequirement};
use crate::report::BenchmarkReport;
use crate::runs::BenchmarkRun;
use async_trait::async_trait;
use futures::StreamExt;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::store::Store;
use rdf_fusion::{Query, QueryOptions, QueryResults};
use reqwest::Url;
use std::fs;
use std::fs::File;
use std::path::PathBuf;
use tokio::time::Instant;

/// The [Berlin SPARQL Benchmark](http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/)
/// is a widely adopted benchmark built around an e-commerce use case.
///
/// This version of the benchmark uses the [pre-prepared datasets](https://zenodo.org/records/12663333)
/// from Oxigraph.
pub struct BsbmExploreBenchmark {
    name: BenchmarkName,
    dataset_size: BsbmDatasetSize,
    max_query_count: Option<u64>,
}

impl BsbmExploreBenchmark {
    /// Creates a new [BsbmExploreBenchmark] with the given sizes.
    pub fn new(dataset_size: BsbmDatasetSize, max_query_count: Option<u64>) -> Self {
        let name = BenchmarkName::Bsbm {
            dataset_size,
            max_query_count,
        };
        Self {
            name,
            dataset_size,
            max_query_count,
        }
    }

    /// The BSBM also generates many queries that are tailored to the generated data. This method
    /// returns a list of queries that should be executed during this run.
    fn list_operations(
        &self,
        env: &RdfFusionBenchContext,
    ) -> anyhow::Result<Vec<BsbmExplorOperation>> {
        println!("Loading queries ...");

        let queries_path = env
            .join_data_dir(PathBuf::from(format!("explore-{}.csv", self.dataset_size)).as_path())?;
        let result = match self.max_query_count {
            None => list_raw_operations(&queries_path)?
                .filter_map(parse_query)
                .collect(),
            Some(max_query_count) => list_raw_operations(&queries_path)?
                .filter_map(parse_query)
                .take(usize::try_from(max_query_count)?)
                .collect(),
        };

        println!("Queries loaded.");
        Ok(result)
    }

    async fn prepare_store(&self, bench_context: &BenchmarkContext<'_>) -> anyhow::Result<Store> {
        println!("Creating in-memory store and loading data ...");
        let data_path = bench_context
            .parent()
            .join_data_dir(PathBuf::from(format!("dataset-{}.nt", self.dataset_size)).as_path())?;
        let data = fs::read(data_path)?;
        let memory_store = Store::new();
        memory_store
            .load_from_reader(RdfFormat::NTriples, data.as_slice())
            .await?;
        println!("Store created and data loaded.");
        Ok(memory_store)
    }
}

#[async_trait]
impl Benchmark for BsbmExploreBenchmark {
    fn name(&self) -> BenchmarkName {
        self.name
    }

    #[allow(clippy::expect_used)]
    fn requirements(&self) -> Vec<PrepRequirement> {
        let dataset_size = self.dataset_size;
        let download_bsbm_tools = PrepRequirement::FileDownload {
            url: Url::parse("https://sourceforge.net/projects/bsbmtools/files/bsbmtools/bsbmtools-0.2/bsbmtools-v0.2.zip/download")
                .expect("parse dataset-name"),
            file_name: PathBuf::from("bsbmtools"),
            action: Some(FileDownloadAction::Unpack(ArchiveType::Zip)),
        };
        let generate_dataset = PrepRequirement::RunCommand {
            workdir: PathBuf::from("./bsbmtools"),
            program: "./generate".to_owned(),
            args: vec![
                "-fc".to_owned(),
                "-pc".to_owned(),
                format!("{}", dataset_size),
                "-dir".to_owned(),
                "../td_data".to_owned(),
                "-fn".to_owned(),
                format!("../dataset-{}", dataset_size),
            ],
            check_requirement: Box::new(move || {
                let exists = File::open(format!("./data/dataset-{dataset_size}.nt")).is_ok();
                Ok(exists)
            }),
        };
        let download_pregenerated_queries = PrepRequirement::FileDownload {
            url: Url::parse("https://zenodo.org/records/12663333/files/explore-1000.csv.bz2")
                .expect("parse dataset-name"),
            file_name: PathBuf::from("explore-1000.csv"),
            action: Some(FileDownloadAction::Unpack(ArchiveType::Bz2)),
        };

        vec![
            download_bsbm_tools,
            generate_dataset,
            download_pregenerated_queries,
        ]
    }

    async fn execute(
        &self,
        bench_context: &BenchmarkContext<'_>,
    ) -> anyhow::Result<Box<dyn BenchmarkReport>> {
        let operations = self.list_operations(bench_context.parent())?;
        let memory_store = self.prepare_store(bench_context).await?;
        let report = execute_benchmark(operations, &memory_store).await?;
        Ok(Box::new(report))
    }
}

fn parse_query(query: BsbmExploreRawOperation) -> Option<BsbmExplorOperation> {
    match query {
        BsbmExploreRawOperation::Query(name, query) => {
            // TODO remove once describe is supported
            if query.contains("DESCRIBE") {
                None
            } else {
                Some(BsbmExplorOperation::Query(
                    name,
                    Query::parse(&query, None).unwrap(),
                ))
            }
        }
    }
}

async fn execute_benchmark(
    operations: Vec<BsbmExplorOperation>,
    memory_store: &Store,
) -> anyhow::Result<ExploreReport> {
    println!("Evaluating queries ...");

    let mut report = ExploreReportBuilder::new();
    let len = operations.len();
    for (idx, operation) in operations.iter().enumerate() {
        if idx % 25 == 0 {
            println!("Progress: {idx}/{len}");
        }

        run_operation(&mut report, memory_store, operation).await?;
    }
    let report = report.build();

    println!("Progress: {len}/{len}");
    println!("All queries evaluated.");

    Ok(report)
}

/// TODO
async fn run_operation(
    report: &mut ExploreReportBuilder,
    store: &Store,
    operation: &BsbmExplorOperation,
) -> anyhow::Result<()> {
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()?;
    let start = Instant::now();

    let options = QueryOptions;
    let name = match operation {
        BsbmExplorOperation::Query(name, q) => {
            match store.query_opt(q.clone(), options.clone()).await? {
                QueryResults::Boolean(_) => (),
                QueryResults::Solutions(mut s) => {
                    while let Some(s) = s.next().await {
                        s?;
                    }
                }
                QueryResults::Graph(mut g) => {
                    while let Some(t) = g.next().await {
                        t?;
                    }
                }
            }
            *name
        }
    };

    let run = BenchmarkRun {
        duration: start.elapsed(),
        report: Some(guard.report().build()?),
    };
    report.add_run(name, run);

    Ok(())
}
