use crate::benchmarks::bsbm::explore::{BsbmExploreQueryName, BSBM_EXPLORE_QUERIES};
use crate::report::BenchmarkReport;
use crate::runs::{BenchmarkRun, BenchmarkRuns};
use crate::utils::write_flamegraph;
use anyhow::{bail, Context};
use datafusion::physical_plan::displayable;
use prettytable::{row, Table};
use rdf_fusion::QueryExplanation;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;

/// Stores the final report of executing a BSBM explore benchmark.
pub struct ExploreReport {
    /// Stores all runs of the benchmark grouped by the query name.
    /// A single query name can have multiple instances (with random variables) in BSBM.
    runs: HashMap<BsbmExploreQueryName, BenchmarkRuns>,
    /// Query explanations for each run.
    explanations: Vec<QueryExplanation>,
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

    fn write_query_results(&self, output_directory: &Path, index: usize) -> anyhow::Result<()> {
        let query_i_path = output_directory.join(format!("query{index}"));
        fs::create_dir_all(&query_i_path).context("Cannot create query directory")?;

        let summary_file = query_i_path.join("0_summary.txt");
        let initial_logical_plan_file = query_i_path.join("1_initial_logical_plan.txt");
        let optimized_logical_plan_file = query_i_path.join("2_optimized_logical_plan.txt");
        let execution_plan_file = query_i_path.join("3_execution_plan.txt");

        let explanation = self
            .explanations
            .get(index)
            .context("Cannot get explanation")?;

        // Write the initial logical plan
        fs::write(
            &summary_file,
            format!("Planning Time:{:?}", explanation.planning_time),
        )
        .with_context(|| {
            format!(
                "Failed to write summary plan to {}",
                initial_logical_plan_file.display()
            )
        })?;

        // Write the initial logical plan
        let initial_logical_plan = explanation.initial_logical_plan.to_string();
        fs::write(
            &initial_logical_plan_file,
            format!("Initial Logical Plan:\n\n{initial_logical_plan}"),
        )
        .with_context(|| {
            format!(
                "Failed to write initial logical plan to {}",
                initial_logical_plan_file.display()
            )
        })?;

        // Write the optimized logical plan
        let optimized_logical_plan = explanation.optimized_logical_plan.to_string();
        fs::write(
            &optimized_logical_plan_file,
            format!("Optimized Logical Plan:\n\n{optimized_logical_plan}"),
        )
        .with_context(|| {
            format!(
                "Failed to write optimized logical plan to {}",
                optimized_logical_plan_file.display()
            )
        })?;

        // Write the execution plan
        let execution_plan = displayable(explanation.execution_plan.as_ref()).indent(false);
        fs::write(
            &execution_plan_file,
            format!("Execution Plan:\n\n{execution_plan}"),
        )
        .with_context(|| {
            format!(
                "Failed to write execution plan to {}",
                execution_plan_file.display()
            )
        })?;

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

        if !self.explanations.is_empty() {
            let queries_path = output_dir.join("queries");
            fs::create_dir_all(&queries_path).context("Cannot create queries directory")?;
            for i in 0..self.explanations.len() {
                self.write_query_results(&queries_path, i)?;
            }
        }

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
                explanations: Vec::new(),
            },
        }
    }

    /// Adds a run to a particular query.
    pub(super) fn add_run(&mut self, name: BsbmExploreQueryName, run: BenchmarkRun) {
        let runs = self.report.runs.entry(name).or_default();
        runs.add_run(run);
    }

    /// Adds an explanation for a particular query.
    ///
    /// It is expected that the n-th call of this method is the explanation of the n-th query.
    pub(super) fn add_explanation(&mut self, explanation: QueryExplanation) {
        self.report.explanations.push(explanation)
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
