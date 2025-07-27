use anyhow::{Context, bail};
use std::path::Path;
use std::process::Command;

/// Executes a shell command in a specified working directory.
pub fn prepare_run_command(
    workdir: &Path,
    program: &str,
    args: &Vec<String>,
) -> anyhow::Result<()> {
    println!("Working directory: {}", workdir.display());
    println!("Executing command: {} {}", program, args.join(" "));
    let mut cmd = Command::new(program);
    cmd.args(args).current_dir(workdir);

    let status = cmd
        .status()
        .context(format!("Failed to execute command '{program}'."))?;

    if !status.success() {
        let exit_code = status.code().unwrap_or(-1);
        bail!("Command `{}` failed with exit code {}", program, exit_code);
    }

    println!("Command executed successfully.");
    Ok(())
}
