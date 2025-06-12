use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

mod explore;

pub use explore::BsbmExploreBenchmark;

/// Indicates the size of the dataset.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize, ValueEnum)]
pub enum BsbmDatasetSize {
    #[value(name = "1000")]
    N1_000,
    #[value(name = "2500")]
    N2_500,
    #[value(name = "5000")]
    N5_000,
    #[value(name = "7500")]
    N7_500,
    #[value(name = "10000")]
    N10_000,
    #[value(name = "25000")]
    N25_000,
    #[value(name = "50000")]
    N50_000,
    #[value(name = "75000")]
    N75_000,
    #[value(name = "250000")]
    N250_000,
    #[value(name = "500000")]
    N500_000,
}

impl Display for BsbmDatasetSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            BsbmDatasetSize::N1_000 => "1000",
            BsbmDatasetSize::N2_500 => "2500",
            BsbmDatasetSize::N5_000 => "5000",
            BsbmDatasetSize::N7_500 => "7500",
            BsbmDatasetSize::N10_000 => "10000",
            BsbmDatasetSize::N25_000 => "25000",
            BsbmDatasetSize::N50_000 => "50000",
            BsbmDatasetSize::N75_000 => "75000",
            BsbmDatasetSize::N250_000 => "250000",
            BsbmDatasetSize::N500_000 => "500000",
        };
        write!(f, "{string}")
    }
}
