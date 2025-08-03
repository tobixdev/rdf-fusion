use std::fmt::Display;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum WindFarmQueryName {
    GroupedProduction1,
    GroupedProduction2,
    GroupedProduction3,
    GroupedProduction4,
    MultiGrouped1,
    MultiGrouped2,
    MultiGrouped3,
    MultiGrouped4,
    ProductionQuery1,
    ProductionQuery2,
    ProductionQuery3,
    ProductionQuery4,
}

impl WindFarmQueryName {
    pub fn list_queries() -> Vec<Self> {
        vec![
            WindFarmQueryName::GroupedProduction1,
            WindFarmQueryName::GroupedProduction2,
            WindFarmQueryName::GroupedProduction3,
            WindFarmQueryName::GroupedProduction4,
            WindFarmQueryName::MultiGrouped1,
            WindFarmQueryName::MultiGrouped2,
            WindFarmQueryName::MultiGrouped3,
            WindFarmQueryName::MultiGrouped4,
            WindFarmQueryName::ProductionQuery1,
            WindFarmQueryName::ProductionQuery2,
            WindFarmQueryName::ProductionQuery3,
            WindFarmQueryName::ProductionQuery4,
        ]
    }

    pub fn file_name(&self) -> &'static str {
        match self {
            WindFarmQueryName::GroupedProduction1 => "grouped_production_query1.sparql",
            WindFarmQueryName::GroupedProduction2 => "grouped_production_query2.sparql",
            WindFarmQueryName::GroupedProduction3 => "grouped_production_query3.sparql",
            WindFarmQueryName::GroupedProduction4 => "grouped_production_query4.sparql",
            WindFarmQueryName::MultiGrouped1 => "multi_grouped_query1.sparql",
            WindFarmQueryName::MultiGrouped2 => "multi_grouped_query2.sparql",
            WindFarmQueryName::MultiGrouped3 => "multi_grouped_query3.sparql",
            WindFarmQueryName::MultiGrouped4 => "multi_grouped_query4.sparql",
            WindFarmQueryName::ProductionQuery1 => "production_query1.sparql",
            WindFarmQueryName::ProductionQuery2 => "production_query2.sparql",
            WindFarmQueryName::ProductionQuery3 => "production_query3.sparql",
            WindFarmQueryName::ProductionQuery4 => "production_query4.sparql",
        }
    }
}

impl Display for WindFarmQueryName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
