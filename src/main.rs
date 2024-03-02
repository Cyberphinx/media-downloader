use csv::ReaderBuilder;
use eyre::Result;
use futures::{stream, StreamExt};
use reqwest::{header::CONTENT_TYPE, Client};

/// This function accepts the download url with apikey param,
/// the filename with extension, and reqwest http client
async fn get(url: &str, filename: &str, client: &Client) -> Result<()> {
    let response = client.get(url).send().await?;

    if response.status().is_success() {
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .map(|v| v.to_str().unwrap_or_default())
            .unwrap_or_default();

        println!("Content-Type: {}", content_type);

        let data = response.bytes().await?;
        tokio::fs::write(format!("export/{}", filename), &data).await?;
        Ok(())
    } else {
        Err(eyre::Report::msg(format!(
            "Failed to download {}: HTTP Error {}",
            url,
            response.status()
        )))
    }
}

/// This function instantiate a new reqwest http client and then calls the get function
async fn download_file(url: &str, filename: &str) -> Result<()> {
    let client = Client::new();
    get(url, &format!("{}.mp3", filename), &client).await
}

/// This function reads the csv and collects the file download url and filename columns
async fn download_urls_from_csv(
    csv_path: &str,
    api_key: &str,
    url_column_name: &str,
    filename_column_name: &str,
) -> Result<()> {
    let mut rdr = ReaderBuilder::new().from_path(csv_path)?;
    let headers = rdr.headers()?.clone();

    let url_column = headers
        .iter()
        .position(|h| h == url_column_name)
        .ok_or_else(|| eyre::Report::msg("Url column name not found"))?;

    let filename_column = headers
        .iter()
        .position(|h| h == filename_column_name)
        .ok_or_else(|| eyre::Report::msg("Filename column name not found"))?;

    let records: Vec<(String, String)> = rdr
        .records()
        .filter_map(Result::ok)
        .map(|r| {
            let url = format!(
                "{}?apikey={}",
                r.get(url_column).unwrap_or_default(),
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
                println!("Downloaded {} as {}.mp3", url, filename);
            }
        })
        .await;

    Ok(())
}

/// This is the main entry point of the program, it reads environment variables
#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let api_key = std::env::var("APIKEY").expect("Missing env APIKEY");
    let csv_path = std::env::var("CSV_PATH").expect("Missing env CSV_PATH");
    let url_column_name = "recording_url";
    let filename_column_name = "date";
    download_urls_from_csv(&csv_path, &api_key, url_column_name, filename_column_name).await?;
    Ok(())
}
