use crate::benchmarks::windfarm::generate::write_prefixes;
use anyhow::Context;
use chrono::{NaiveDate, NaiveDateTime};
use rand::SeedableRng;
use rand::distr::{Distribution, Uniform};
use rand::prelude::StdRng;
use rand_distr::{LogNormal, Normal};
use std::io::Write;

/// Generates the time series data for the wind farm (Chrontext) benchmark.
///
/// This includes:
/// - Energy production data (double) for each turbine generator.
/// - Operating status data (boolean) for each turbine.
/// - Wind direction (double) for each turbine
/// - Wind speed (double) for each turbine
pub fn generate_time_series<W: Write>(
    writer: &mut W,
    num_turbines: usize,
) -> anyhow::Result<()> {
    write_prefixes(writer)?;

    let mut rng = StdRng::seed_from_u64(0);
    let uniform = Uniform::new(0.0, 1.0)?;

    let mut init_ep_values = create_initial_ep_values(&mut rng, num_turbines)?;
    let operating_values: Vec<bool> = (0..num_turbines)
        .map(|_| uniform.sample(&mut rng) > 0.01)
        .collect();
    let mut init_wdir_values = create_initial_wdir_values(&mut rng, num_turbines)?;
    let mut init_wsp_values = create_initial_wsp_values(&mut rng, num_turbines)?;

    const DAYS: [(u32, u32); 3] = [(8, 29), (8, 30), (9, 1)];
    for (month, day) in DAYS {
        for hour in 0..24 {
            println!("Generating data for 2022-{month}-{day} hour {hour}");
            let timestamps = create_timestamps(month, day, hour);

            generate_energy_production_for_hour(
                writer,
                &timestamps,
                &mut init_ep_values,
                &operating_values,
                &mut rng,
                num_turbines,
            )?;
            generate_operating_status_for_hour(writer, &timestamps, &operating_values)?;
            generate_wind_direction_for_hour(
                writer,
                &timestamps,
                &mut init_wdir_values,
                &mut rng,
            )?;
            generate_wind_speed_for_hour(
                writer,
                &timestamps,
                &mut init_wsp_values,
                &mut rng,
            )?;
        }
    }

    Ok(())
}

/// Creates initial energy production values for each turbine.
fn create_initial_ep_values(
    rng: &mut StdRng,
    num_turbines: usize,
) -> anyhow::Result<Vec<f64>> {
    const MAX_POWER_VALUES: [u32; 3] = [5_000_000, 10_000_000, 15_000_000];
    let uniform = Uniform::new(0.0, 1.0)?;
    let result = (0..num_turbines)
        .map(|i| {
            let max_power = MAX_POWER_VALUES[i % MAX_POWER_VALUES.len()] as f64;
            max_power * uniform.sample(rng)
        })
        .collect();
    Ok(result)
}

/// Creates initial wind direction values for each turbine.
fn create_initial_wdir_values(
    rng: &mut StdRng,
    num_turbines: usize,
) -> anyhow::Result<Vec<f64>> {
    let uniform = Uniform::new(0.0, 360.0)?;
    let result = (0..num_turbines).map(|_| uniform.sample(rng)).collect();
    Ok(result)
}

/// Creates initial wind speed values for each turbine.
fn create_initial_wsp_values(
    rng: &mut StdRng,
    num_turbines: usize,
) -> anyhow::Result<Vec<f64>> {
    let log_normal =
        LogNormal::new(2.0, 0.5).context("Failed to create log-normal distribution")?;
    Ok((0..num_turbines).map(|_| log_normal.sample(rng)).collect())
}

/// Creates start and end NaiveDateTime for a given hour.
fn create_timestamps(month: u32, day: u32, hour: u32) -> Vec<NaiveDateTime> {
    let start = NaiveDate::from_ymd_opt(2022, month, day)
        .unwrap()
        .and_hms_opt(hour, 0, 0)
        .unwrap();
    let end = NaiveDate::from_ymd_opt(2022, month, day)
        .unwrap()
        .and_hms_opt(hour, 59, 59)
        .unwrap();

    let mut timestamps = Vec::new();
    let mut current = start;
    while current <= end {
        timestamps.push(current);
        current += chrono::Duration::seconds(10);
    }
    timestamps
}

/// Generates energy production data for a single hour.
fn generate_energy_production_for_hour<W: Write>(
    writer: &mut W,
    timestamps: &[NaiveDateTime],
    init_ep_values: &mut [f64],
    operating_values: &[bool],
    rng: &mut StdRng,
    n_turbines: usize,
) -> anyhow::Result<()> {
    let normal_dist = Normal::new(0.0, 1000.0)?;

    for (i, (init_ep, &is_operating)) in
        init_ep_values.iter_mut().zip(operating_values).enumerate()
    {
        if !is_operating {
            continue;
        }

        let turbine_id = (i + 1) % n_turbines;
        let mut current_ep = *init_ep;

        for timestamp in timestamps {
            let delta = normal_dist.sample(rng);
            current_ep += delta;

            write!(
                writer,
                r#"
wpex:w{turbine_id} ct:hasDataPoint [
    ct:hasTimestamp "{timestamp}"^^xsd:dateTime ;
    ct:hasValue "{value}"^^xsd:double ] ."#,
                timestamp = timestamp.format("%Y-%m-%dT%H:%M:%S"),
                value = current_ep,
                turbine_id = turbine_id
            )?;
        }
        *init_ep = current_ep; // Update initial value for the next hour
    }
    Ok(())
}

/// Generates operating status data for a single hour.
fn generate_operating_status_for_hour<W: Write>(
    writer: &mut W,
    timestamps: &[NaiveDateTime],
    operating_values: &[bool],
) -> anyhow::Result<()> {
    for (i, &is_operating) in operating_values.iter().enumerate() {
        let turbine_id = i + 1;
        for timestamp in timestamps {
            write!(
                writer,
                r#"
wpex:oper{turbine_id} ct:hasDataPoint [
    ct:hasTimestamp "{timestamp}"^^xsd:dateTime ;
    ct:hasValue "{value}"^^xsd:boolean ] ."#,
                timestamp = timestamp.format("%Y-%m-%dT%H:%M:%S"),
                value = is_operating,
                turbine_id = turbine_id
            )?;
        }
    }
    Ok(())
}

/// Generates wind direction data for a single hour.
fn generate_wind_direction_for_hour<W: Write>(
    writer: &mut W,
    timestamps: &[NaiveDateTime],
    init_wdir_values: &mut [f64],
    rng: &mut StdRng,
) -> anyhow::Result<()> {
    let normal_dist =
        Normal::new(0.0, 3.6).context("Failed to create normal distribution")?;

    for (i, init_wdir) in init_wdir_values.iter_mut().enumerate() {
        let turbine_id = i + 1;
        let mut current_wdir = *init_wdir;

        for timestamp in timestamps {
            let delta = normal_dist.sample(rng);
            current_wdir += delta;
            // Ensure the value stays within the 0-360 degree range
            current_wdir = (current_wdir % 360.0 + 360.0) % 360.0;

            let rdf_text = format!(
                r#"wpex:wdir{turbine_id} ct:hasDataPoint [
    ct:hasTimestamp "{timestamp}"^^xsd:dateTime ;
    ct:hasValue "{value}"^^xsd:double ] ."#,
                timestamp = timestamp.format("%Y-%m-%dT%H:%M:%S"),
                value = current_wdir,
                turbine_id = turbine_id
            );
            writer.write_all(rdf_text.as_bytes())?;
        }
        *init_wdir = current_wdir; // Update initial value for the next hour
    }
    Ok(())
}

/// Generates wind speed data for a single hour.
fn generate_wind_speed_for_hour<W: Write>(
    writer: &mut W,
    timestamps: &[NaiveDateTime],
    init_wsp_values: &mut [f64],
    rng: &mut StdRng,
) -> anyhow::Result<()> {
    let normal_dist =
        Normal::new(0.0, 1.0).context("Failed to create normal distribution")?;

    for (i, init_wsp) in init_wsp_values.iter_mut().enumerate() {
        let turbine_id = i + 1;
        let mut current_wsp = *init_wsp;

        for timestamp in timestamps {
            let delta = normal_dist.sample(rng);
            current_wsp += delta;
            // Ensure the value is always positive
            current_wsp = current_wsp.abs();

            let rdf_text = format!(
                r#"wpex:wsp{turbine_id} ct:hasDataPoint [
    ct:hasTimestamp "{timestamp}"^^xsd:dateTime ;
    ct:hasValue "{value}"^^xsd:double ] ."#,
                timestamp = timestamp.format("%Y-%m-%dT%H:%M:%S"),
                value = current_wsp,
                turbine_id = turbine_id
            );
            writer.write_all(rdf_text.as_bytes())?;
        }
        *init_wsp = current_wsp; // Update initial value for the next hour
    }
    Ok(())
}
