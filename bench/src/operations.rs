use rdf_fusion::Query;
use std::fs;
use std::path::Path;

#[allow(clippy::panic)]
#[allow(clippy::panic_in_result_fn)]
pub fn list_raw_operations(
    path: &Path,
) -> anyhow::Result<impl Iterator<Item = SparqlRawOperation>> {
    let reader = fs::read(path)?;
    let result = csv::Reader::from_reader(reader.as_slice())
        .records()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .rev()
        .map(|l| match &l[1] {
            "query" => SparqlRawOperation::Query(l[2].into()),
            "update" => SparqlRawOperation::Update(l[2].into()),
            _ => panic!("Unexpected operation kind {}", &l[1]),
        });
    Ok(result)
}

#[allow(dead_code)]
#[derive(Clone)]
pub enum SparqlRawOperation {
    Query(String),
    Update(String),
}

#[allow(clippy::large_enum_variant, clippy::allow_attributes)]
#[derive(Clone)]
pub enum SparqlOperation {
    Query(Query),
}
