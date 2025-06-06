use crate::benchmarks::{Benchmark, BenchmarkName};
use crate::environment::{Bencher, BenchmarkingContext};
use crate::operations::{list_raw_operations, SparqlOperation, SparqlRawOperation};
use crate::prepare::PrepRequirement::FileDownload;
use crate::prepare::{FileDownloadAction, PrepRequirement};
use async_trait::async_trait;
use clap::ValueEnum;
use futures::StreamExt;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::store::Store;
use rdf_fusion::{Query, QueryOptions, QueryResults};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::PathBuf;

/// The [Berlin SPARQL Benchmark](http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/)
/// is a widely adopted benchmarks that is built around an e-commerce use case.
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
    fn list_operations(&self, env: &BenchmarkingContext) -> anyhow::Result<Vec<SparqlOperation>> {
        let queries_path = env
            .join_data_dir(PathBuf::from(format!("explore-{}.csv", self.dataset_size)).as_path())?;

        Ok(match self.max_query_count {
            None => list_raw_operations(&queries_path)?
                .filter_map(parse_query)
                .collect(),
            Some(max_query_count) => list_raw_operations(&queries_path)?
                .filter_map(parse_query)
                .take(usize::try_from(max_query_count)?)
                .collect(),
        })
    }
}

#[async_trait]
impl Benchmark for BsbmExploreBenchmark {
    fn name(&self) -> BenchmarkName {
        self.name
    }

    fn requirements(&self) -> Vec<PrepRequirement> {
        vec![
            create_file_download(&format!("dataset-{}.nt", self.dataset_size)),
            create_file_download(&format!("explore-{}.csv", self.dataset_size)),
        ]
    }

    async fn execute(&self, bencher: &mut Bencher<'_>) -> anyhow::Result<()> {
        println!("Loading queries ...");
        let operations = self.list_operations(bencher.context())?;
        println!("Queries loaded.");

        println!("Creating in-memory store and loading data ...");
        let data_path = bencher
            .context()
            .join_data_dir(PathBuf::from(format!("dataset-{}.nt", self.dataset_size)).as_path())?;
        let data = fs::read(data_path)?;
        let memory_store = Store::new();
        memory_store
            .load_from_reader(RdfFormat::NTriples, data.as_slice())
            .await?;
        println!("Store created and data loaded.");

        println!("Evaluating queries ...");
        let result = bencher
            .bench(async || {
                let len = operations.len();
                for (idx, operation) in operations.iter().enumerate() {
                    if idx % 25 == 0 {
                        println!("Progress: {idx}/{len}");
                    }
                    run_operation(&memory_store, operation).await;
                }
                Ok(())
            })
            .await;
        println!("All queries evaluated.");

        result
    }
}

fn parse_query(query: SparqlRawOperation) -> Option<SparqlOperation> {
    match query {
        SparqlRawOperation::Query(q) => {
            // TODO remove once describe is supported
            if q.contains("DESCRIBE") {
                None
            } else {
                Some(SparqlOperation::Query(Query::parse(&q, None).unwrap()))
            }
        }
        SparqlRawOperation::Update(_) => None,
    }
}

async fn run_operation(store: &Store, operation: &SparqlOperation) {
    let options = QueryOptions;
    match operation {
        SparqlOperation::Query(q) => {
            match store.query_opt(q.clone(), options.clone()).await.unwrap() {
                QueryResults::Boolean(_) => (),
                QueryResults::Solutions(mut s) => {
                    while let Some(s) = s.next().await {
                        s.unwrap();
                    }
                }
                QueryResults::Graph(mut g) => {
                    while let Some(t) = g.next().await {
                        t.unwrap();
                    }
                }
            }
        }
    }
}

#[allow(clippy::expect_used)]
fn create_file_download(file: &str) -> PrepRequirement {
    FileDownload {
        url: Url::parse(&format!(
            "https://zenodo.org/records/12663333/files/{file}.bz2",
        ))
        .expect("parse dataset-name"),
        file_name: PathBuf::from(file),
        action: Some(FileDownloadAction::UnpackBz2),
    }
}

/// Indicates the size of the dataset.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize, ValueEnum)]
pub enum BsbmDatasetSize {
    #[value(name = "1000")]
    N1000,
    #[value(name = "5000")]
    N5000,
}

impl Display for BsbmDatasetSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BsbmDatasetSize::N1000 => f.write_str("1000"),
            BsbmDatasetSize::N5000 => f.write_str("5000"),
        }
    }
}
