use crate::benchmarks::bsbm::use_case::{BsbmUseCase, BsbmUseCaseName};
use clap::ValueEnum;
use std::fmt::{Display, Formatter};

/// The BSBM Business Intelligence Use Case.
pub struct BusinessIntelligenceUseCase;

impl BsbmUseCase for BusinessIntelligenceUseCase {
    type QueryName = BsbmBusinessIntelligenceQueryName;

    fn name() -> BsbmUseCaseName {
        BsbmUseCaseName::BusinessIntelligence
    }

    fn list_queries() -> Vec<Self::QueryName> {
        vec![
            BsbmBusinessIntelligenceQueryName::Q1,
            BsbmBusinessIntelligenceQueryName::Q2,
            BsbmBusinessIntelligenceQueryName::Q3,
            BsbmBusinessIntelligenceQueryName::Q4,
            BsbmBusinessIntelligenceQueryName::Q5,
            BsbmBusinessIntelligenceQueryName::Q6,
            BsbmBusinessIntelligenceQueryName::Q7,
            BsbmBusinessIntelligenceQueryName::Q8,
        ]
    }
}

/// The BSBM business intelligence query names.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum BsbmBusinessIntelligenceQueryName {
    Q1,
    Q2,
    Q3,
    Q4,
    Q5,
    Q6,
    Q7,
    Q8,
}

impl TryFrom<u8> for BsbmBusinessIntelligenceQueryName {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(BsbmBusinessIntelligenceQueryName::Q1),
            2 => Ok(BsbmBusinessIntelligenceQueryName::Q2),
            3 => Ok(BsbmBusinessIntelligenceQueryName::Q3),
            4 => Ok(BsbmBusinessIntelligenceQueryName::Q4),
            5 => Ok(BsbmBusinessIntelligenceQueryName::Q5),
            6 => Ok(BsbmBusinessIntelligenceQueryName::Q6),
            7 => Ok(BsbmBusinessIntelligenceQueryName::Q7),
            8 => Ok(BsbmBusinessIntelligenceQueryName::Q8),
            _ => Err(anyhow::anyhow!(
                "Invalid BSBM Business Intelligence query name: {}",
                value
            )),
        }
    }
}

impl Display for BsbmBusinessIntelligenceQueryName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            BsbmBusinessIntelligenceQueryName::Q1 => "Q1",
            BsbmBusinessIntelligenceQueryName::Q2 => "Q2",
            BsbmBusinessIntelligenceQueryName::Q3 => "Q3",
            BsbmBusinessIntelligenceQueryName::Q4 => "Q4",
            BsbmBusinessIntelligenceQueryName::Q5 => "Q5",
            BsbmBusinessIntelligenceQueryName::Q6 => "Q6",
            BsbmBusinessIntelligenceQueryName::Q7 => "Q7",
            BsbmBusinessIntelligenceQueryName::Q8 => "Q8",
        };
        write!(f, "{string}")
    }
}
