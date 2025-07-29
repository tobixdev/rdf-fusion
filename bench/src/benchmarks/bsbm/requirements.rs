use crate::benchmarks::bsbm::NumProducts;
use crate::prepare::{ArchiveType, FileDownloadAction, PrepRequirement};
use anyhow::bail;
use reqwest::Url;
use std::fs::File;
use std::path::PathBuf;

/// Downloads the BSBM tools from a GitHub fork.
pub fn download_bsbm_tools() -> PrepRequirement {
    PrepRequirement::FileDownload {
        url: Url::parse("https://github.com/Tpt/bsbm-tools/archive/59d0a8a605b26f21506789fa1a713beb5abf1cab.zip")
            .expect("parse dataset-name"),
        file_name: PathBuf::from("bsbmtools"),
        action: Some(FileDownloadAction::Unpack(ArchiveType::Zip)),
    }
}

/// Calls the BSBM tools to generate the dataset.
pub fn generate_dataset_requirement(
    file_name: PathBuf,
    num_products: NumProducts,
) -> PrepRequirement {
    let file_name_str = file_name.display().to_string();
    PrepRequirement::RunCommand {
        workdir: PathBuf::from("./bsbmtools"),
        program: "./generate".to_owned(),
        args: vec![
            "-fc".to_owned(), // We do not support RDFS reasoning
            "-pc".to_owned(), // Product Count
            format!("{}", num_products),
            "-dir".to_owned(),
            "../td_data".to_owned(),
            "-fn".to_owned(),
            format!("../{}", &file_name_str[..file_name_str.len() - 3]), // The script appends .nt
        ],
        check_requirement: Box::new(move |ctx| {
            let path = ctx.join_data_dir(&file_name)?;
            if File::open(&path).is_err() {
                bail!("File {} does not exist", path.display());
            }
            Ok(())
        }),
    }
}

/// Downloads the pre-generated queries from Oxigraph.
pub fn download_pre_generated_queries(
    use_case: &str,
    file_name: PathBuf,
    num_products: NumProducts,
) -> PrepRequirement {
    PrepRequirement::FileDownload {
        url: Url::parse(&format!(
            "https://zenodo.org/records/12663333/files/{use_case}-{num_products}.csv.bz2"
        ))
        .expect("parse dataset-name"),
        file_name,
        action: Some(FileDownloadAction::Unpack(ArchiveType::Bz2)),
    }
}
