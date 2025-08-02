use crate::benchmarks::windfarm::generate::generate_static;
use crate::benchmarks::windfarm::NumberOfWindTurbines;
use crate::benchmarks::{Benchmark, BenchmarkName};
use crate::environment::BenchmarkContext;
use crate::prepare::PrepRequirement;
use crate::report::BenchmarkReport;
use anyhow::Context;
use async_trait::async_trait;
use rdf_fusion::io::{RdfFormat, RdfSerializer};
use std::fs::File;
use std::path::PathBuf;

/// Holds file paths for the files required for executing a BSBM run.
struct WindfarmFilePaths {
    /// A path to the wind farm data NTriples file.
    wind_farm_data: PathBuf,
    /// A path to the time series NTriples file.
    time_series_data: PathBuf,
    /// A path to the folder that contains all the queries.
    query_folder: PathBuf,
}

/// The "Wind Farm" benchmark is derived from the benchmarks used to evaluate Chrontext [1], an
/// ontology-based data access framework for time series data.
///
/// Based on the original benchmark, we have implemented a data generator in Rust that ...
/// - generates the RDF triples for the original static data (e.g., wind farm sites, turbines)
/// - generates the time series data as RDF triples instead of CSV files
///
/// As a result, these data can be used with any regular triple store.
///
/// # References
///
/// [1] M. Bakken and A. Soylu, “Chrontext: Portable SPARQL queries over contextualised time
///     series data in industrial settings,” Expert Systems with Applications, vol. 226, p. 120149,
///     Sept. 2023, doi: 10.1016/j.eswa.2023.120149.
pub struct WindFarmBenchmark {
    name: BenchmarkName,
    num_turbines: NumberOfWindTurbines,
    paths: WindfarmFilePaths,
}

impl WindFarmBenchmark {
    /// Creates a new [WindFarmBenchmark] with the given sizes.
    pub fn new(num_turbines: NumberOfWindTurbines) -> Self {
        let name = BenchmarkName::WindFarm { num_turbines };

        let wind_farm_data = PathBuf::from("./windfarm.nt".to_string());
        let time_series_data = PathBuf::from("./timeseries.nt".to_string());
        let query_folder =
            PathBuf::from("./benchmark-docker/queries_chrontext/".to_string());
        let paths = WindfarmFilePaths {
            wind_farm_data,
            time_series_data,
            query_folder,
        };
        Self {
            name,
            num_turbines,
            paths,
        }
    }
}

#[async_trait]
impl Benchmark for WindFarmBenchmark {
    fn name(&self) -> BenchmarkName {
        self.name
    }

    #[allow(clippy::expect_used)]
    fn requirements(&self) -> Vec<PrepRequirement> {
        let num_turbines = self.num_turbines.into_usize();
        let wind_farm_data_path = self.paths.wind_farm_data.clone();
        let generate_dataset = PrepRequirement::RunClosure {
            execute: Box::new(move |_| {
                let wind_farm_file = File::create(&wind_farm_data_path)
                    .context("Could not create file for static wind farm data")?;

                let mut rdf_writer = RdfSerializer::from_format(RdfFormat::NTriples)
                    .for_writer(&wind_farm_file);
                generate_static(&mut rdf_writer, num_turbines)
            }),
            check_requirement: Box::new(move |_| {
                File::open(format!("windfarm{num_turbines}.nt",))
                    .context("Could not open file for static wind farm data")?;
                Ok(())
            }),
        };

        vec![generate_dataset]
    }

    async fn execute(
        &self,
        _bench_context: &BenchmarkContext<'_>,
    ) -> anyhow::Result<Box<dyn BenchmarkReport>> {
        todo!()
    }
}
