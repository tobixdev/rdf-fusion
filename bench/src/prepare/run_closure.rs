use crate::environment::RdfFusionBenchContext;

/// Executes a closure.
pub fn prepare_run_closure(
    context: &RdfFusionBenchContext,
    closure: &dyn Fn(&RdfFusionBenchContext) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    closure(context)
}
