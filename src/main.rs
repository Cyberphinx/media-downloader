use downloader::{
    download_audios::download_urls_from_csv, download_transcripts::download_transcripts_from_csv,
};

use eyre::Result;

/// This is the main entry point of the program, it reads environment variables
#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let api_key = std::env::var("APIKEY").expect("Missing env APIKEY");
    let csv_path = std::env::var("CSV_PATH").expect("Missing env CSV_PATH");
    let base_url = std::env::var("BASE_URL").expect("Missing env BASE_URL");

    let url_column_name = "recording_url";
    let filename_column_name = "date";
    let call_id_column_name = "call_id";

    // download content-type: media/mpeg
    download_urls_from_csv(&csv_path, &api_key, url_column_name, filename_column_name).await?;

    // download content-type: application/json
    download_transcripts_from_csv(
        &csv_path,
        &api_key,
        &base_url,
        call_id_column_name,
        filename_column_name,
    )
    .await?;

    Ok(())
}
