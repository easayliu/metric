//! 收 OTLP 指标写入 ClickHouse。
//!
//! ```bash
//! export CH_ENDPOINT=http://127.0.0.1:8123
//! export CH_DATABASE=logs
//! export CH_TABLE=otel_metric
//! export CH_USER=default CH_PASSWORD=
//! cargo run --example otlp_to_clickhouse -- --ddl | clickhouse-client   # 先建表
//! cargo run --example otlp_to_clickhouse
//! ```

use std::time::Duration;

use metricpipe::batch::{BatchConfig, RetryConfig};
use metricpipe::sink::ClickhouseSink;
use metricpipe::source::OtlpSource;
use metricpipe::{MetricEvent, Pipeline};

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

#[tokio::main]
async fn main() -> metricpipe::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut sink = ClickhouseSink::new(
        env("CH_ENDPOINT", "http://127.0.0.1:8123"),
        env("CH_DATABASE", "logs"),
        env("CH_TABLE", "otel_metric"),
    )
    .timezone(chrono_tz::Asia::Shanghai)
    .timeout(Duration::from_secs(30));

    if let (Ok(user), Ok(password)) = (std::env::var("CH_USER"), std::env::var("CH_PASSWORD")) {
        sink = sink.auth(user, password);
    }

    if std::env::args().any(|arg| arg == "--ddl") {
        println!("{};", sink.create_table_ddl());
        return Ok(());
    }

    Pipeline::builder()
        .source(OtlpSource::new())
        // 给每个数据点补一个集群名，方便多集群共用一张表
        .transform(|mut event: MetricEvent| {
            event.insert("cluster", env("CLUSTER", "local"));
            Some(event)
        })
        .sink(sink)
        .batch(
            BatchConfig::default()
                .max_events(20_000)
                .timeout(Duration::from_secs(2)),
        )
        .retry(RetryConfig::default())
        // ClickHouse 不可用时不启动，否则收进来的数据点全在内存里等着
        .require_healthy(true)
        .build()?
        .run()
        .await
}
