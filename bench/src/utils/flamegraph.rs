use anyhow::Context;
use pprof::Frames;
use pprof::flamegraph::Options;
use std::collections::HashMap;
use std::fmt::Write;

/// Creates a flamegraph from the given data.
///
/// This code is partly copied from pprof-rs and included here because we create the flamegraph from
/// the aggregated frame information and not from a single pprof report (which includes timing
/// information).
pub fn write_flamegraph<W: std::io::Write>(
    writer: W,
    data: &HashMap<Frames, isize>,
) -> anyhow::Result<()> {
    let lines: Vec<String> = data
        .iter()
        .map(|(key, value)| {
            let mut line = key.thread_name_or_id();
            line.push(';');

            for frame in key.frames.iter().rev() {
                for symbol in frame.iter().rev() {
                    write!(&mut line, "{symbol};")?;
                }
            }

            line.pop().unwrap_or_default();
            write!(&mut line, " {value}")?;

            Ok(line)
        })
        .collect::<anyhow::Result<_>>()?;

    if !lines.is_empty() {
        let mut options = Options::default();
        pprof::flamegraph::from_lines(&mut options, lines.iter().map(|s| &**s), writer)
            .context("Cannot create flamegraph")?;
    }

    Ok(())
}
