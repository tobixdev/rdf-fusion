/// Executes a closure.
pub fn prepare_run_closure(
    closure: &dyn Fn() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    closure()
}
