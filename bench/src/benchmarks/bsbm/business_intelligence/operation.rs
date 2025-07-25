use crate::benchmarks::bsbm::business_intelligence::BsbmBusinessIntelligenceQueryName;
use rdf_fusion::Query;
use std::fs;
use std::path::Path;

#[allow(clippy::panic)]
#[allow(clippy::panic_in_result_fn)]
#[allow(clippy::expect_used)]
pub(super) fn list_raw_operations(
    path: &Path,
) -> anyhow::Result<impl Iterator<Item = BsbmBusinessIntelligenceRawOperation>> {
    let reader = fs::read(path)?;
    let result = csv::Reader::from_reader(reader.as_slice())
        .records()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|record| {
            let query_id = record[0].parse::<u8>().expect("Can't parse query id");
            let query_name =
                BsbmBusinessIntelligenceQueryName::try_from(query_id).expect("Invalid query id");

            match &record[1] {
                "query" => {
                    BsbmBusinessIntelligenceRawOperation::Query(query_name, record[2].into())
                }
                _ => panic!("Unexpected operation kind {}", &record[1]),
            }
        });
    Ok(result)
}

#[allow(dead_code)]
#[derive(Clone)]
pub(super) enum BsbmBusinessIntelligenceRawOperation {
    Query(BsbmBusinessIntelligenceQueryName, String),
}

#[allow(clippy::large_enum_variant, clippy::allow_attributes)]
#[derive(Clone)]
pub(super) enum BsbmBusinessIntelligenceOperation {
    Query(BsbmBusinessIntelligenceQueryName, Query),
}
