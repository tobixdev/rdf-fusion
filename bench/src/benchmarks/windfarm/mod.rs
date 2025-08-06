mod benchmark;
mod generate;
mod queries;
mod report;

pub use benchmark::WindFarmBenchmark;
pub use benchmark::get_wind_farm_raw_sparql_operation;
use clap::ValueEnum;
pub use queries::WindFarmQueryName;
use std::fmt::{Display, Formatter};

/// Indicates the size of the dataset.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum NumberOfWindTurbines {
    #[value(name = "4")]
    N4,
    #[value(name = "16")]
    N16,
    #[value(name = "100")]
    N100,
    #[value(name = "400")]
    N400,
}

impl NumberOfWindTurbines {
    /// Returns the number of turbines as usize.
    pub fn into_usize(self) -> usize {
        match self {
            NumberOfWindTurbines::N4 => 4,
            NumberOfWindTurbines::N16 => 16,
            NumberOfWindTurbines::N100 => 100,
            NumberOfWindTurbines::N400 => 400,
        }
    }
}

impl Display for NumberOfWindTurbines {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            NumberOfWindTurbines::N4 => "4",
            NumberOfWindTurbines::N16 => "16",
            NumberOfWindTurbines::N100 => "100",
            NumberOfWindTurbines::N400 => "400",
        };
        write!(f, "{string}")
    }
}
