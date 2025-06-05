use crate::environment::BenchmarkingContext;
use crate::prepare::FileDownloadAction;
use anyhow::{bail, Context};
use bzip2::read::MultiBzDecoder;
use reqwest::Url;
use std::{fs, path};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

pub fn ensure_file_download(env: &BenchmarkingContext, file_name: PathBuf) -> anyhow::Result<()> {
    let file_path = env.join_data_dir(&file_name)?;
    if !file_path.exists() {
        bail!("{:?} does not exist ({:?})", &file_path, &path::absolute(&file_path));
    }
    Ok(())
}

pub async fn prepare_file_download(
    env: &BenchmarkingContext,
    url: Url,
    file_name: PathBuf,
    action: Option<FileDownloadAction>,
) -> anyhow::Result<()> {
    let file_path = env.join_data_dir(&file_name)?;
    if file_path.exists() {
        fs::remove_file(&file_path)?;
    }

    let response = reqwest::Client::new()
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("Could not send request to download file '{url}'"))?;

    if !response.status().is_success() {
        bail!(
            "Response code for file '{url}' was not OK. Actual: {}",
            response.status()
        )
    }

    fs::create_dir_all(file_path.parent().context("Path should be a file")?)?;
    fs::write(&file_path, &response.bytes().await?)?;

    match action {
        None => {}
        Some(FileDownloadAction::UnpackBz2) => {
            let mut buf = Vec::new();
            MultiBzDecoder::new(File::open(&file_path)?).read_to_end(&mut buf)?;
            fs::write(&file_path, &buf)?;
        }
    }

    Ok(())
}
