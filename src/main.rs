use std::{fs, time};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::anyhow;
use clap::{App, Arg, ArgMatches};
use futures::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use percent_encoding::percent_decode_str;
use reqwest::Client;
use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = make_app();
    let matches = app.get_matches();
    validate_matches(&matches)?;

    // ダウンロードリストの作成
    let urls = if matches.is_present("url") {
        let url = matches.value_of("url").unwrap();
        let start = matches.value_of("start").unwrap().parse().unwrap();
        let end = matches.value_of("end").unwrap().parse().unwrap();
        create_sequential_download_list(url, start, end)?
    } else {
        let filepath = matches.value_of("list").unwrap();
        create_download_list_from_file(filepath)?
    };

    // 出力先ディレクトリの準備
    let output_dir = PathBuf::from(matches.value_of("output").unwrap());
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir)?;
    }

    // プログレスバーの準備
    let bars = Arc::new(MultiProgress::new());

    let main_bar = Arc::new(bars.add(ProgressBar::new(urls.len() as u64)));
    main_bar.set_style(create_main_bar_style());

    // ダウンロード準備
    let connections: usize = matches.value_of("connections").unwrap().parse().unwrap();
    let connections = if connections <= 10 {
        connections
    } else {
        2
    };
    let urls = Arc::new(urls);
    let semaphore = Arc::new(Semaphore::new(connections));
    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(connections);

    // メインバーを更新するスレッドを生成
    tokio::spawn({
        let main_bar = main_bar.clone();
        async move {
            loop {
                main_bar.tick();
                if main_bar.is_finished() {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }
        }
    });

    // ダウンロードジョブを処理するスレッドを生成
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

    main_bar.finish();

    Ok(())
}

/// コマンドライン アプリケーションの初期化
fn make_app() -> App<'static> {
    App::new("mikanbako")
        .author("(C) 2021 Manapia.")
        .version("1.1.1")
        .arg(Arg::new("url")
            .long("url")
            .short('u')
            .long_about("The URL that will be the source for generating the serial number. The placeholder {} is replaced with a number.")
            .takes_value(true))
        .arg(Arg::new("list")
            .long("list")
            .short('l')
            .about("The path of the file to read the list of URLs")
            .takes_value(true))
        .arg(Arg::new("start")
            .long("start")
            .short('s')
            .about("Sequence start number")
            .validator(validate_number)
            .default_value("1"))
        .arg(Arg::new("end")
            .long("end")
            .short('e')
            .about("Sequence end number")
            .takes_value(true)
            .validator(validate_number))
        .arg(Arg::new("output")
            .long("output")
            .short('o')
            .about("The path of the directory where you want to save the file")
            .default_value("./"))
        .arg(Arg::new("connections")
            .long("connections")
            .short('c')
            .about("The number of connections to download in parallel")
            .validator(validate_natural_number)
            .default_value("2"))
        .after_help("The end argument is required when the url argument is specified.
When the argument list is specified, the start and end arguments are ignored.
If you specify both the url and list arguments, the url is processed.")
}

/// コマンドライン引数を検証し、エラーを返します。
fn validate_matches(matches: &ArgMatches) -> anyhow::Result<()> {
    if matches.is_present("url") {
        if !matches.is_present("start") {
            return Err(anyhow!("The argument start is required if the argument url is specified."));
        }
        if !matches.is_present("end") {
            return Err(anyhow!("The argument end is required if the argument url is specified."));
        }
    } else if !matches.is_present("list") {
        return Err(anyhow!("Specify either the url argument or the list argument."));
    }

    Ok(())
}

/// 連番の URL リストを作成します。
fn create_sequential_download_list<S>(url: S, start: i64, end: i64) -> anyhow::Result<Vec<String>>
    where S: Into<String>
{
    let url = url.into();

    if start > end {
        return Err(anyhow!("The value of start must be less than end"));
    }

    let mut urls = Vec::with_capacity((end - start) as usize + 1);
    for i in start..=end {
        let url = url.replace("{}", &i.to_string());
        urls.push(url);
    }

    Ok(urls)
}

/// 指定したファイルを読み込み、ダウンロードする URL のリストを返します。
fn create_download_list_from_file(filepath: impl AsRef<Path>) -> anyhow::Result<Vec<String>> {
    let fp = fs::File::open(&filepath.as_ref())?;
    let mut reader = BufReader::new(fp);

    let mut files = Vec::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        if line.len() != 0 {
            files.push(line.replace("\r", "").replace("\n", ""));
        }
    }

    Ok(files)
}

/// 指定した URL からデータをダウンロードして、指定したディレクトリに保存します。
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

/// メイン プログレスバーのスタイルを返します。
fn create_main_bar_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("{elapsed_precise} [{bar:40.cyan/blue}] {pos} / {len} {percent:>3$}%")
        .progress_chars("=>-")
}

/// プログレスバーのスタイルを返します。
fn create_bar_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("{elapsed_precise} [{bar:40.cyan/blue}] {percent:>3$}% {binary_bytes_per_sec} {bytes} {msg}")
        .progress_chars("=>-")
}

/// URL からファイル名が特定できない場合にランダムなファイル名を生成します。
fn gen_filename() -> anyhow::Result<String> {
    let now = time::SystemTime::now();
    let secs = now.duration_since(time::UNIX_EPOCH)?.as_millis();

    Ok(secs.to_string())
}

/// 渡された文字列を i64 としてパースを試します。
fn validate_number(v: &str) -> Result<(), String> {
    match v.parse::<i64>() {
        Ok(_) => Ok(()),
        Err(_) => Err(format!("The value must be an integer")),
    }
}

/// 渡された文字列を usize としてパースを試します。
fn validate_natural_number(v: &str) -> Result<(), String> {
    match v.parse::<usize>() {
        Ok(_) => Ok(()),
        Err(_) => Err(format!("The value must be a positive integer")),
    }
}
