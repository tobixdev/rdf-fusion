use crate::benchmarks::bsbm::explore::BsbmExploreQueryName;
use rdf_fusion::Query;
use std::fs;
use std::path::Path;

#[allow(clippy::panic)]
#[allow(clippy::panic_in_result_fn)]
#[allow(clippy::expect_used)]
pub(super) fn list_raw_operations(
    path: &Path,
) -> anyhow::Result<impl Iterator<Item = BsbmExploreRawOperation>> {
    let reader = fs::read(path)?;
    let result = csv::Reader::from_reader(reader.as_slice())
        .records()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter_map(|record| {
            let query_id = record[0].parse::<u8>().expect("Can't parse query id");
            let query_name =
                BsbmExploreQueryName::try_from(query_id).expect("Invalid query id");

            match &record[1] {
                "query" => {
                    Some(BsbmExploreRawOperation::Query(query_name, record[2].into()))
                }
                "update" => None,
                _ => panic!("Unexpected operation kind {}", &record[1]),
            }
        });
    Ok(result)
}

#[allow(dead_code)]
#[derive(Clone)]
pub(super) enum BsbmExploreRawOperation {
    Query(BsbmExploreQueryName, String),
}

#[allow(clippy::large_enum_variant, clippy::allow_attributes)]
#[derive(Clone)]
pub(super) enum BsbmExploreOperation {
    Query(BsbmExploreQueryName, Query),
}
