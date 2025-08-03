mod generate_static;
mod generate_time_series;

pub use generate_static::generate_static;
pub use generate_time_series::generate_time_series;
use std::io::Write;

fn write_prefixes<W: Write>(writer: &mut W) -> anyhow::Result<()> {
    write!(
        writer,
        "\
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix ct: <https://github.com/magbak/chrontext#>.
@prefix rds: <https://github.com/magbak/chrontext/rds_power#>.
@prefix wpex: <https://github.com/magbak/chrontext/windpower_example#> .
    "
    )?;
    Ok(())
}
