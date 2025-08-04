use crate::environment::BenchmarkContext;

/// Executes a closure.
pub fn prepare_run_closure(
    context: &BenchmarkContext,
    closure: &dyn Fn(&BenchmarkContext) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    closure(context)
}
