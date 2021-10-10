use std::{fs, time};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::anyhow;
use clap::{App, Arg};
use futures::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use percent_encoding::percent_decode_str;
use reqwest::Client;
use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = App::new("mikanbako")
        .author("(C) 2021 Manapia.")
        .arg(Arg::new("url")
            .long("url")
            .short('u')
            .takes_value(true)
            .required(true))
        .arg(Arg::new("start")
            .long("start")
            .short('s')
            .validator(validate_number)
            .default_value("1"))
        .arg(Arg::new("end")
            .long("end")
            .short('e')
            .takes_value(true)
            .validator(validate_number)
            .required(true))
        .arg(Arg::new("output")
            .long("output")
            .short('o')
            .default_value("./"))
        .arg(Arg::new("connections")
            .long("connections")
            .short('c')
            .validator(validate_natural_number)
            .default_value("2"));

    let matches = app.get_matches();

    // パラメータの取得
    let url = matches.value_of("url").unwrap();
    let start: i64 = matches.value_of("start").unwrap().parse().unwrap();
    let end: i64 = matches.value_of("end").unwrap().parse().unwrap();
    let output = matches.value_of("output").unwrap();
    let connections: usize = matches.value_of("connections").unwrap().parse().unwrap();

    if start > end {
        return Err(anyhow!("The value of start must be less than end"));
    }

    // ダウンロードリストの作成
    let mut urls = Vec::with_capacity((end - start) as usize);
    for i in start..=end {
        let url = url.replace("{}", &i.to_string());
        urls.push(url);
    }

    // 出力先ディレクトリの準備
    let output_dir = PathBuf::from(output);

    if !output_dir.exists() {
        fs::create_dir_all(&output_dir)?;
    }

    // プログレスバーの準備
    let bars = Arc::new(MultiProgress::new());

    let main_bar = bars.add(ProgressBar::new(urls.len() as u64));
    main_bar.set_style(ProgressStyle::default_bar()
        .template("{elapsed_precise} [{bar:40.cyan/blue}] {pos} / {len} {percent:>3$}%")
        .progress_chars("=>-"));
    main_bar.tick();

    // ダウンロード準備
    let urls = Arc::new(urls);
    let semaphore = Arc::new(Semaphore::new(connections));
    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(connections);

    for _ in 0..connections {
        let semaphore = semaphore.clone();
        let counter = counter.clone();
        let urls = urls.clone();
        let bars = bars.clone();
        let output_dir = output_dir.clone();
        let main_bar = main_bar.clone();

        handles.push(tokio::spawn(async move {
            loop {
                let _ = semaphore.acquire().await.unwrap();

                let index = counter.fetch_add(1, Ordering::Acquire);
                if index >= urls.len() {
                    break;
                }
                let url = &urls[index];

                let bar = bars.add(ProgressBar::new_spinner());

                if let Err(err) = download(url, &output_dir, &bar).await {
                    eprintln!("{:#?}", err);
                }

                bars.remove(&bar);
                main_bar.inc(1);
            }
        }));
    }

    // すべてのタスクが終了するまで待機
    for handle in handles {
        handle.await?;
    }

    Ok(())
}

async fn download(
    url: impl Into<String>,
    output_dir: impl AsRef<Path>,
    bar: &ProgressBar,
) -> anyhow::Result<()> {
    let url = url.into();
    let client = Client::new();

    // 接続を確立
    let res = match client.get(url).send().await {
        Ok(v) => v,
        Err(err) => {
            println!("Error while downloading file {:?}", err);
            return Err(anyhow!("{:?}", err));
        }
    };

    // サイズの取得
    let content_length = res.content_length().unwrap_or_else(|| 0);

    // 出力先ファイルの準備
    let filename = match res.url().path_segments() {
        Some(segments) => percent_decode_str(
            segments.last().unwrap()).decode_utf8_lossy().to_string(),
        None => gen_filename()?,
    };
    let filepath = output_dir.as_ref().join(&filename);
    let mut file = fs::OpenOptions::new()
        .create(true).write(true).truncate(true).open(filepath)?;

    // プログレスバーの再初期化
    if content_length != 0 {
        bar.set_message(filename);
        bar.set_length(content_length);
        bar.set_style(create_bar_style());
    }

    // ダウンロード中
    let mut stream = res.bytes_stream();
    while let Some(item) = stream.next().await {
        let chunk = item.or(Err(anyhow!("Error while downloading file")))?;

        file.write(&chunk)?;

        bar.inc(chunk.len() as u64);
    }

    // ダウンロード完了
    bar.finish();

    Ok(())
}

fn create_bar_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("{elapsed_precise} [{bar:40.cyan/blue}] {percent:>3$}% {binary_bytes_per_sec} {bytes} {msg}")
        .progress_chars("=>-")
}

/// URL からファイル名が特定できない場合にランダムなファイル名を生成します
fn gen_filename() -> anyhow::Result<String> {
    let now = time::SystemTime::now();
    let secs = now.duration_since(time::UNIX_EPOCH)?.as_millis();

    Ok(secs.to_string())
}

fn validate_number(v: &str) -> Result<(), String> {
    match v.parse::<i64>() {
        Ok(_) => Ok(()),
        Err(_) => Err(format!("The value must be an integer")),
    }
}

fn validate_natural_number(v: &str) -> Result<(), String> {
    match v.parse::<usize>() {
        Ok(_) => Ok(()),
        Err(_) => Err(format!("The value must be a positive integer")),
    }
}
