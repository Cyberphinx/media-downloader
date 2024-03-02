use std::fs;

use csv::ReaderBuilder;
use eyre::Result;
use futures::{stream, StreamExt};
use reqwest::{header::CONTENT_TYPE, Client};

/// This function accepts the download url with apikey param,
/// the filename with extension, and reqwest http client
async fn get(url: &str, filename: &str, client: &Client) -> Result<()> {
    if let Ok(metadata) = fs::metadata(format!("export/transcripts/{}", filename)) {
        Err(eyre::Report::msg(format!(
            "File already exist with size {}",
            metadata.len()
        )))
    } else {
        let response = client
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await?;

        if response.status().is_success() {
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .map(|v| v.to_str().unwrap_or_default())
                .unwrap_or_default();

            println!("Content-Type: {}", content_type);

            let data = response.bytes().await?;

            tokio::fs::write(format!("export/transcripts/{}", filename), &data).await?;
            Ok(())
        } else {
            Err(eyre::Report::msg(format!(
                "Failed to download {}: HTTP Error {}",
                url,
                response.status()
            )))
        }
    }
}

/// This function instantiate a new reqwest http client and then calls the get function
async fn download_file(url: &str, filename: &str) -> Result<()> {
    let client = Client::new();
    get(url, &format!("{}.json", filename), &client).await
}

pub async fn download_transcripts_from_csv(
    csv_path: &str,
    api_key: &str,
    base_url: &str,
    call_id_column_name: &str,
    filename_column_name: &str,
) -> Result<()> {
    let mut rdr = ReaderBuilder::new().from_path(csv_path)?;
    let headers = rdr.headers()?.clone();

    let call_id_column = headers
        .iter()
        .position(|h| h == call_id_column_name)
        .ok_or_else(|| eyre::Report::msg("Call id column name not found"))?;

    let filename_column = headers
        .iter()
        .position(|h| h == filename_column_name)
        .ok_or_else(|| eyre::Report::msg("Filename column name not found"))?;

    let records: Vec<(String, String)> = rdr
        .records()
        .filter_map(Result::ok)
        .map(|r| {
            let url = format!(
                "{}/{}?apikey={}",
                base_url,
                r.get(call_id_column).unwrap_or_default(),
                api_key
            );
            let filename = r
                .get(filename_column)
                .unwrap_or_default()
                .replace(' ', "_")
                .to_string();
            (url, filename)
        })
        .collect();

    stream::iter(records.into_iter().enumerate())
        .for_each_concurrent(None, |(_index, (url, filename))| async move {
            if let Err(e) = download_file(&url, &filename).await {
                eprintln!("Failed to download {}: {}", url, e);
            } else {
                println!("Downloaded {} as {}", url, filename);
            }
        })
        .await;

    Ok(())
}
