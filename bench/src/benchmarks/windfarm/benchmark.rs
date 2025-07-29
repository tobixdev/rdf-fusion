use crate::benchmarks::windfarm::generate::generate_static;
use crate::benchmarks::windfarm::NumberOfWindTurbines;
use crate::benchmarks::{Benchmark, BenchmarkName};
use crate::environment::BenchmarkContext;
use crate::prepare::PrepRequirement;
use crate::report::BenchmarkReport;
use async_trait::async_trait;
use rdf_fusion::io::{RdfFormat, RdfSerializer};
use std::fs::File;

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
}

impl WindFarmBenchmark {
    /// Creates a new [WindFarmBenchmark] with the given sizes.
    pub fn new(num_turbines: NumberOfWindTurbines) -> Self {
        let name = BenchmarkName::WindFarm { num_turbines };
        Self { name, num_turbines }
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
        let generate_dataset = PrepRequirement::RunClosure {
            execute: Box::new(move || {
                let dataset_file =
                    File::create(format!("./data/windfarm-static-{num_turbines}.nt",))
                        .expect("Could not create file");
                let mut rdf_writer = RdfSerializer::from_format(RdfFormat::NTriples)
                    .for_writer(&dataset_file);
                generate_static(&mut rdf_writer, num_turbines)
            }),
            check_requirement: Box::new(move || {
                let exists =
                    File::open(format!("./data/windfarm-static-{num_turbines}.nt",))
                        .is_ok();
                Ok(exists)
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
