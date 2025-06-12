use crate::benchmarks::bsbm::explore::{BsbmExploreQueryName, BSBM_EXPLORE_QUERIES};
use crate::report::BenchmarkReport;
use crate::runs::{BenchmarkRun, BenchmarkRuns};
use crate::utils::write_flamegraph;
use anyhow::{bail, Context};
use prettytable::{row, Table};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;

/// Stores the final report of executing a BSBM explore benchmark.
pub struct ExploreReport {
    /// Stores all runs of the benchmark grouped by the query name.
    /// A single query name can have multiple instances (with random variables) in BSBM.
    runs: HashMap<BsbmExploreQueryName, BenchmarkRuns>,
}

impl ExploreReport {
    /// Writes a tabular summary of the query execution time.
    fn write_summary<W: Write + ?Sized>(&self, writer: &mut W) -> anyhow::Result<()> {
        // Create the table
        let mut table = Table::new();
        table.add_row(row!["Query", "Samples", "Average Duration"]);
        for query in BSBM_EXPLORE_QUERIES {
            let summary = self
                .runs
                .get(&query)
                .map(BenchmarkRuns::summarize)
                .transpose()?;

            let samples = summary
                .as_ref()
                .map_or_else(|| "-".to_owned(), |s| s.number_of_samples.to_string());
            let average_duration = summary
                .as_ref()
                .map_or_else(|| "-".to_owned(), |s| format!("{:?}", s.avg_duration));

            table.add_row(row![query.to_string(), samples, average_duration]);
        }
        table.print(writer)?;

        Ok(())
    }

    /// Write aggregated flamegraph.
    fn write_aggregated_flamegraphs(&self, output_directory: &Path) -> anyhow::Result<()> {
        if !output_directory.is_dir() {
            bail!(
                "Output directory {} does not exist",
                output_directory.display()
            );
        }

        for query in BSBM_EXPLORE_QUERIES {
            let frames = self
                .runs
                .get(&query)
                .map(BenchmarkRuns::accumulate_profiles)
                .transpose()?;
            if let Some(frames) = frames {
                let flamegraph_file = output_directory.join(format!("{query}.svg"));
                let mut flamegraph_file =
                    fs::File::create(flamegraph_file).context("Cannot create flamegraph file")?;
                write_flamegraph(&mut flamegraph_file, &frames)?;
            }
        }

        Ok(())
    }
}

impl BenchmarkReport for ExploreReport {
    fn write_results(&self, output_dir: &Path) -> anyhow::Result<()> {
        let summary_txt = output_dir.join("summary.txt");
        let mut summary_file = fs::File::create(summary_txt)?;
        self.write_summary(&mut summary_file)?;

        let flamegraphs_dir = output_dir.join("flamegraphs");
        fs::create_dir_all(&flamegraphs_dir)
            .context("Cannot create flamegraphs directory before writing flamegraphs")?;
        self.write_aggregated_flamegraphs(&flamegraphs_dir)?;

        Ok(())
    }
}

/// Builder for the [`ExploreReport`].
///
/// This should only be accessible to the benchmark code.
pub(super) struct ExploreReportBuilder {
    /// The inner report that is being built.
    report: ExploreReport,
}

impl ExploreReportBuilder {
    /// Creates a new builder.
    pub(super) fn new() -> Self {
        Self {
            report: ExploreReport {
                runs: HashMap::new(),
            },
        }
    }

    /// Adds a run to a particular query.
    pub(super) fn add_run(&mut self, name: BsbmExploreQueryName, run: BenchmarkRun) {
        let runs = self.report.runs.entry(name).or_default();
        runs.add_run(run);
    }

    /// Finalizes the report.
    pub(super) fn build(self) -> ExploreReport {
        self.report
    }
}

impl Default for ExploreReportBuilder {
    fn default() -> Self {
        Self::new()
    }
}
