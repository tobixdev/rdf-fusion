#![allow(clippy::print_stdout)]
use anyhow::Result;
use clap::Parser;
use rdf_fusion_testsuite::evaluator::TestEvaluator;
use rdf_fusion_testsuite::manifest::TestManifest;
use rdf_fusion_testsuite::report::build_report;

#[derive(Parser)]
/// Oxigraph testsuite runner
struct Args {
    /// URI of the testsuite manifest(s) to run
    manifest: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = Args::parse();

    let evaluator = TestEvaluator;
    let manifest = TestManifest::new(matches.manifest);
    let results = evaluator.evaluate(manifest).await?;
    print!("{}", build_report(results));
    Ok(())
}
