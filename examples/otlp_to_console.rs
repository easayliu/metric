//! 收 OTLP 指标打到控制台，不需要任何外部依赖，最快验证 SDK 那边配对了没有。
//!
//! ```bash
//! cargo run --example otlp_to_console            # gRPC 4317 + HTTP 4318
//! cargo run --example otlp_to_console -- text    # 一行一个数据点的摘要
//! ```
//!
//! 然后把应用的 `OTEL_EXPORTER_OTLP_ENDPOINT` 指到 `http://127.0.0.1:4317`。

use std::time::Duration;

use metricpipe::batch::BatchConfig;
use metricpipe::sink::console::Encoding;
use metricpipe::sink::ConsoleSink;
use metricpipe::source::OtlpSource;
use metricpipe::Pipeline;

#[tokio::main]
async fn main() -> metricpipe::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let encoding = match std::env::args().nth(1).as_deref() {
        Some("text") => Encoding::Text,
        _ => Encoding::Json,
    };

    Pipeline::builder()
        .source(OtlpSource::new())
        .sink(ConsoleSink::new(encoding))
        .batch(BatchConfig::default().timeout(Duration::from_millis(200)))
        .build()?
        .run()
        .await
}
