use async_dependency_installer_for_r::{FetchRequest, Fetcher};
use std::env;
use std::io::{self, Read};
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    let input = match args.get(1) {
        Some(path) => std::fs::read_to_string(path)
            .map_err(|err| format!("failed to read request file {}: {err}", path))?,
        None => read_stdin().map_err(|err| format!("failed to read stdin: {err}"))?,
    };

    let request: FetchRequest =
        serde_json::from_str(&input).map_err(|err| format!("invalid request JSON: {err}"))?;
    let response = Fetcher::default().fetch_all(request).await;
    let payload =
        serde_json::to_string_pretty(&response).map_err(|err| format!("encode response: {err}"))?;

    if let Some(path) = output_path(&args[2..]) {
        std::fs::write(&path, payload)
            .map_err(|err| format!("failed to write response file {}: {err}", path.display()))?;
    } else {
        println!("{payload}");
    }

    Ok(())
}

fn read_stdin() -> io::Result<String> {
    let mut buffer = String::new();
    io::stdin().read_to_string(&mut buffer)?;
    Ok(buffer)
}

fn output_path(extra_args: &[String]) -> Option<PathBuf> {
    let mut iter = extra_args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--output" {
            return iter.next().map(PathBuf::from);
        }
    }
    None
}
