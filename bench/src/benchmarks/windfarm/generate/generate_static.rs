use rdf_fusion::io::{RdfFormat, RdfParser, WriterQuadSerializer};
use std::io::Write;

/// Generates the static part of the data for the windfarm (Chrontext) benchmark.
///
/// This includes:
/// - Windfarm Sites
/// - Wind Turbines
/// - Generator Systems
/// - Generators
/// - Weather Measuring Systems
pub fn generate_static<W: Write>(
    serializer: &mut WriterQuadSerializer<W>,
    num_turbines: usize,
) -> anyhow::Result<()> {
    generate_wind_farm_sites(serializer)?;
    generate_wind_turbines(serializer, num_turbines)?;

    Ok(())
}

const WIND_FARM_SITES: [(&str, u32); 4] = [
    ("Wind Mountain", 0),
    ("Gale Valley", 1),
    ("Gusty Plains", 2),
    ("Breezy Field", 3),
];

/// A wind farm site has the following triples:
fn generate_wind_farm_sites<W: Write>(
    serializer: &mut WriterQuadSerializer<W>,
) -> anyhow::Result<()> {
    for (name, iri_idx) in WIND_FARM_SITES {
        let rdf_text = format!(
            r#"
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rds: <https://github.com/magbak/chrontext/rds_power#>.
@prefix wpex: <https://github.com/magbak/chrontext/windpower_example#> .

wpex:Site{iri_idx} rdf:type rds:Site ;
    rdfs:label "{name}" .
"#
        );
        write_text(serializer, &rdf_text)?;
    }

    Ok(())
}

/// Generates `n_turbines` wind turbines.
fn generate_wind_turbines<W: Write>(
    serializer: &mut WriterQuadSerializer<W>,
    n_turbines: usize,
) -> anyhow::Result<()> {
    const MAX_POWER_VALUES: [u32; 3] = [5_000_000, 10_000_000, 15_000_000];
    let turbines_per_site = n_turbines / WIND_FARM_SITES.len();

    for i in 1..=n_turbines {
        let max_power_value = MAX_POWER_VALUES[i % MAX_POWER_VALUES.len()];
        let site_idx = i / turbines_per_site;
        let idx_within_site = i % turbines_per_site;

        let rdf_text = format!(
            r#"
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix ct: <https://github.com/magbak/chrontext#>.
@prefix rds: <https://github.com/magbak/chrontext/rds_power#>.
@prefix wpex: <https://github.com/magbak/chrontext/windpower_example#> .

wpex:WindTurbine{i} rdf:type rds:A ;
    rdfs:label "Wind turbine {i}" ;
    ct:hasTimeSeries wpex:oper{i} ;
    ct:hasStaticProperty wpex:WindTurbineMaximumPower{i} .

wpex:oper{i} ct:hasExternalId "oper{i}" ;
    ct:hasDatatype xsd:boolean ;
    rdfs:label "Operating" .

wpex:WindTurbineMaximumPower{i} rdfs:label "MaximumPower" ;
    ct:hasStaticValue "{max_power_value}"^^xsd:integer .

wpex:Site{site_idx} rds:hasFunctionalAspect wpex:WindTurbineFunctionalAspect{i} .
wpex:WindTurbineFunctionalAspect{i} rds:hasFunctionalAspectNode wpex:WindTurbine{i} ;
    rdfs:label "A{idx_within_site}" .
"#
        );
        write_text(serializer, &rdf_text)?;
    }

    Ok(())
}

/// Writes the quads in the `rdf_text` into the `serializer`.
fn write_text<W: Write>(
    serializer: &mut WriterQuadSerializer<W>,
    rdf_text: &str,
) -> anyhow::Result<()> {
    let quads = RdfParser::from_format(RdfFormat::Turtle).for_reader(rdf_text.as_bytes());
    for quad in quads {
        let quad = quad?;
        serializer.serialize_quad(quad.as_ref())?;
    }
    Ok(())
}
