//! ClickHouse 走的是 HTTP 接口，这里直接对着一个假服务端看发出去的原始请求。

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use flate2::read::GzDecoder;
use metricpipe::event::FIXED_COLUMNS;
use metricpipe::sink::{ClickhouseSink, InsertFormat, Sink};
use metricpipe::{Exemplar, MetricEvent, MetricType};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// 2026-09-07 03:04:08.914293456 UTC
const NOW: u64 = 1_788_750_248_914_293_456;

/// `JSONCompactEachRow` 的请求体：一行一个 JSON 数组。
fn rows(body: &str) -> Vec<Vec<Value>> {
    body.lines()
        .map(|line| {
            serde_json::from_str::<Vec<Value>>(line).unwrap_or_else(|err| panic!("{err}: {line}"))
        })
        .collect()
}

/// 紧凑行里某一列的位置。
fn col(name: &str) -> usize {
    FIXED_COLUMNS
        .iter()
        .position(|c| *c == name)
        .unwrap_or_else(|| panic!("没有 {name} 这列"))
}

fn point() -> MetricEvent {
    MetricEvent {
        timestamp: NOW,
        start_timestamp: NOW - 60_000_000_000,
        metric_name: Arc::from("压缩测试"),
        metric_type: MetricType::Histogram,
        service_name: Arc::from("order-service"),
        count: 3,
        sum: 9.5,
        bucket_counts: vec![1, 2],
        explicit_bounds: vec![5.0],
        exemplars: vec![Exemplar {
            timestamp: NOW + 1,
            value: 7.5,
            trace_id: "e89a476882236ce0f1186d1522c8f59f".into(),
            span_id: "e8b0e73e2132f21c".into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// 收一个**完整**请求（按 Content-Length 把 body 读全），交出请求头文本和 body 原始字节。
async fn capture_full(
    response_body: &'static str,
) -> (String, tokio::task::JoinHandle<(String, Vec<u8>)>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut raw = Vec::new();
        let mut buf = [0u8; 8192];

        let head_end = loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break raw.len();
            }
            raw.extend_from_slice(&buf[..n]);
            if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                break at + 4;
            }
        };
        let head = String::from_utf8_lossy(&raw[..head_end]).to_string();

        let len: usize = head
            .to_lowercase()
            .split("content-length:")
            .nth(1)
            .and_then(|rest| rest.split("\r\n").next())
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0);
        while raw.len() - head_end < len {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
        }
        let body = raw[head_end..].to_vec();

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{response_body}",
            response_body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        (head, body)
    });

    (format!("http://{addr}"), handle)
}

/// 空 body 的 POST（`SELECT 1` 这种健康检查）没有 Content-Length，ClickHouse 回 411。
#[tokio::test]
async fn empty_body_still_carries_content_length_and_is_not_gzipped() {
    let (endpoint, server) = capture_full("1\n").await;
    let sink = ClickhouseSink::new(endpoint, "logs", "otel_metric").timeout(Duration::from_secs(5));

    sink.execute("SELECT 1").await.unwrap();

    let (head, _) = server.await.unwrap();
    let head = head.to_lowercase();
    assert!(head.starts_with("post "), "{head}");
    assert!(head.contains("content-length:"), "411 的坑:\n{head}");
    assert!(
        !head.contains("content-encoding: gzip"),
        "空 body 不该压:\n{head}"
    );
}

/// 声明了 gzip 就得真的是 gzip：解出来必须是原始的 JSONCompactEachRow 行，
/// INSERT 语句里带着和行位置对应的列清单。
#[tokio::test]
async fn insert_body_is_gzipped_and_round_trips() {
    let (endpoint, server) = capture_full("").await;
    let mut sink = ClickhouseSink::new(endpoint, "logs", "otel_metric")
        .timeout(Duration::from_secs(5))
        .extra_columns(vec![(
            "cluster".to_owned(),
            "LowCardinality(String)".to_owned(),
        )]);

    let mut event = point();
    event.insert("cluster", "bj-prod");
    // 攒出好几块，确认分块序列化 + 流式压缩拼出来的还是完整的一份
    let batch: Vec<MetricEvent> = (0..600).map(|_| event.clone()).collect();
    sink.write(&batch).await.unwrap();

    let (head, body) = server.await.unwrap();
    let lower = head.to_lowercase();
    assert!(
        lower.contains("content-encoding: gzip"),
        "没有声明 gzip:\n{head}"
    );
    assert!(lower.contains("content-length:"), "{head}");
    assert!(
        head.contains("date_time_input_format=best_effort"),
        "带偏移的时间戳要开宽松解析:\n{head}"
    );
    assert!(
        head.contains("input_format_null_as_default=1"),
        "NaN 会被 serde 写成 null，不开这个整批都插不进去:\n{head}"
    );
    assert!(
        head.contains("FORMAT+JSONCompactEachRow") || head.contains("FORMAT%20JSONCompactEachRow"),
        "{head}"
    );
    // 列清单在 query 里（反引号 url 编码成 %60），最后一列是静态字段
    assert!(
        head.contains("%60flags%60%2C+%60cluster%60%29")
            || head.contains("%60flags%60%2C%20%60cluster%60%29"),
        "INSERT 要带列清单，静态字段排在固定列后面:\n{head}"
    );
    assert!(
        !head.contains("input_format_skip_unknown_fields"),
        "紧凑格式没有 key，这条设置用不上:\n{head}"
    );

    let mut plain = String::new();
    GzDecoder::new(&body[..])
        .read_to_string(&mut plain)
        .expect("body 应当是合法的 gzip");
    assert!(plain.ends_with('\n'), "每行都要以换行结尾: {plain:?}");
    let rows = rows(&plain);
    assert_eq!(rows.len(), 600);
    for row in &rows {
        assert_eq!(row.len(), FIXED_COLUMNS.len() + 1, "{row:?}");
        assert_eq!(row[col("metric_name")], "压缩测试");
        assert_eq!(row[col("metric_type")], "Histogram");
        assert_eq!(row[col("count")], 3);
        assert_eq!(row[col("bucket_counts")], serde_json::json!([1, 2]));
        assert_eq!(row[FIXED_COLUMNS.len()], "bj-prod");
        // 没配时区按 UTC，仍然带偏移
        assert_eq!(row[col("timestamp")], "2026-09-07 03:04:08.914293456+00:00");
        assert_eq!(
            row[col("exemplars.timestamp")],
            serde_json::json!(["2026-09-07 03:04:08.914293457+00:00"])
        );
    }
}

/// 退路：`JSONEachRow` 还能用，每行带列名，且带上 `input_format_skip_unknown_fields`。
#[tokio::test]
async fn json_each_row_is_still_available() {
    let (endpoint, server) = capture_full("").await;
    let mut sink = ClickhouseSink::new(endpoint, "logs", "otel_metric")
        .timeout(Duration::from_secs(5))
        .compress(false)
        .insert_format(InsertFormat::JsonEachRow);

    sink.write(&[point()]).await.unwrap();

    let (head, body) = server.await.unwrap();
    assert!(
        head.contains("FORMAT+JSONEachRow") || head.contains("FORMAT%20JSONEachRow"),
        "{head}"
    );
    assert!(
        head.contains("input_format_skip_unknown_fields=1"),
        "{head}"
    );
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("\"metric_name\":\"压缩测试\""), "{body}");
    assert!(
        body.contains("\"timestamp\":\"2026-09-07 03:04:08.914293456+00:00\""),
        "{body}"
    );
}

/// NaN / Inf（Prometheus 的 staleness marker、除零得到的速率）在 JSON 里没有写法，
/// serde 写成 null，服务端靠 `input_format_null_as_default` 收成 0 —— 至少不会因为
/// 一个点把整批带走。
#[tokio::test]
async fn non_finite_values_serialize_as_null() {
    let (endpoint, server) = capture_full("").await;
    let mut sink = ClickhouseSink::new(endpoint, "logs", "otel_metric")
        .timeout(Duration::from_secs(5))
        .compress(false);

    let mut event = point();
    event.metric_type = MetricType::Gauge;
    event.value = f64::NAN;
    sink.write(&[event]).await.unwrap();

    let (_, body) = server.await.unwrap();
    let rows = rows(&String::from_utf8_lossy(&body));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][col("value")], Value::Null, "{:?}", rows[0]);
}

/// 中间代理不认压缩 body 时的退路。
#[tokio::test]
async fn compression_can_be_turned_off() {
    let (endpoint, server) = capture_full("").await;
    let mut sink = ClickhouseSink::new(endpoint, "logs", "otel_metric")
        .timeout(Duration::from_secs(5))
        .compress(false);

    sink.write(&[point()]).await.unwrap();

    let (head, body) = server.await.unwrap();
    assert!(
        !head.to_lowercase().contains("content-encoding: gzip"),
        "关掉了还在压:\n{head}"
    );
    let rows = rows(&String::from_utf8_lossy(&body));
    assert_eq!(rows[0][col("metric_name")], "压缩测试");
}

/// 配了 timezone，INSERT 里的时间戳是那个时区的墙上时间 + 偏移。
#[tokio::test]
async fn timezone_puts_wall_clock_and_offset_on_timestamps() {
    let (endpoint, server) = capture_full("").await;
    let mut sink = ClickhouseSink::new(endpoint, "logs", "otel_metric")
        .timeout(Duration::from_secs(5))
        .compress(false)
        .timezone(chrono_tz::Asia::Shanghai);

    sink.write(&[point()]).await.unwrap();

    let (_, body) = server.await.unwrap();
    let rows = rows(&String::from_utf8_lossy(&body));
    let row = &rows[0];
    assert_eq!(row[col("timestamp")], "2026-09-07 11:04:08.914293456+08:00");
    assert_eq!(
        row[col("start_timestamp")],
        "2026-09-07 11:03:08.914293456+08:00"
    );
    assert_eq!(
        row[col("exemplars.timestamp")],
        serde_json::json!(["2026-09-07 11:04:08.914293457+08:00"])
    );
}

/// 按顺序应答多个请求（同一条 keep-alive 连接或多条连接都行），交出每个请求的 query 参数。
async fn serve_sequence(
    responses: Vec<&'static str>,
) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        let mut queries = Vec::new();
        let mut pending = responses.into_iter();
        'conn: while pending.len() > 0 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut raw: Vec<u8> = Vec::new();
            let mut buf = [0u8; 8192];
            for response_body in pending.by_ref() {
                let head_end = loop {
                    if let Some(at) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        break at + 4;
                    }
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        continue 'conn;
                    }
                    raw.extend_from_slice(&buf[..n]);
                };
                let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
                let len: usize = head
                    .to_lowercase()
                    .split("content-length:")
                    .nth(1)
                    .and_then(|rest| rest.split("\r\n").next())
                    .and_then(|value| value.trim().parse().ok())
                    .unwrap_or(0);
                while raw.len() - head_end < len {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    raw.extend_from_slice(&buf[..n]);
                }
                raw.drain(..head_end + len);

                let query = head
                    .split_whitespace()
                    .nth(1)
                    .and_then(|path| path.split("query=").nth(1))
                    .and_then(|rest| rest.split('&').next())
                    .map(percent_decode)
                    .unwrap_or_default();
                queries.push(query);

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{response_body}",
                    response_body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.flush().await.unwrap();
            }
        }
        queries
    });

    (format!("http://{addr}"), handle)
}

fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

/// 表里的全部固定列，`SELECT name, type FROM system.columns ... FORMAT TSV` 的应答。
const COLUMNS: [(&str, &str); 35] = [
    ("timestamp", "DateTime64(9)"),
    ("start_timestamp", "DateTime64(9)"),
    ("metric_name", "LowCardinality(String)"),
    ("metric_type", "LowCardinality(String)"),
    ("metric_unit", "LowCardinality(String)"),
    ("metric_description", "String"),
    ("service_name", "LowCardinality(String)"),
    ("scope_name", "LowCardinality(String)"),
    ("scope_version", "LowCardinality(String)"),
    ("resource_attributes", "JSON"),
    ("attributes", "JSON(max_dynamic_paths=2048)"),
    ("value", "Float64"),
    ("temporality", "LowCardinality(String)"),
    ("is_monotonic", "UInt8"),
    ("count", "UInt64"),
    ("sum", "Float64"),
    ("min", "Float64"),
    ("max", "Float64"),
    ("bucket_counts", "Array(UInt64)"),
    ("explicit_bounds", "Array(Float64)"),
    ("scale", "Int32"),
    ("zero_count", "UInt64"),
    ("zero_threshold", "Float64"),
    ("positive_offset", "Int32"),
    ("positive_bucket_counts", "Array(UInt64)"),
    ("negative_offset", "Int32"),
    ("negative_bucket_counts", "Array(UInt64)"),
    ("quantiles.quantile", "Array(Float64)"),
    ("quantiles.value", "Array(Float64)"),
    ("exemplars.timestamp", "Array(DateTime64(9))"),
    ("exemplars.value", "Array(Float64)"),
    ("exemplars.trace_id", "Array(String)"),
    ("exemplars.span_id", "Array(String)"),
    ("exemplars.attributes", "Array(JSON)"),
    ("flags", "UInt32"),
];

/// 拼出 TSV 应答，`skip` 里的列当成表里没有，`retype` 里的列换个类型。
fn columns_tsv(skip: &[&str], retype: &[(&str, &str)]) -> &'static str {
    let mut out = String::new();
    for (name, ty) in COLUMNS {
        if skip.contains(&name) {
            continue;
        }
        let ty = retype
            .iter()
            .find(|(target, _)| *target == name)
            .map_or(ty, |(_, ty)| *ty);
        out.push_str(&format!("{name}\t{ty}\n"));
    }
    // 假服务端要 'static 的应答，测试进程结束就回收，泄漏这一小段无所谓
    Box::leak(out.into_boxed_str())
}

/// 表存在但列没跟上配置：healthcheck 必须把缺的列点出来（Nested 的子列也算）。
#[tokio::test]
async fn healthcheck_reports_missing_columns() {
    let columns: &'static str = Box::leak(
        format!(
            "{}cluster\tLowCardinality(String)\n",
            columns_tsv(&["exemplars.timestamp"], &[])
        )
        .into_boxed_str(),
    );
    let (endpoint, server) = serve_sequence(vec!["1\n", "1\n", columns]).await;
    let sink = ClickhouseSink::new(endpoint, "logs", "otel_metric")
        .timeout(Duration::from_secs(5))
        .extra_columns(vec![
            ("cluster".to_owned(), "LowCardinality(String)".to_owned()),
            ("env".to_owned(), "LowCardinality(String)".to_owned()),
        ]);

    let err = sink.healthcheck().await.expect_err("缺列应当报错");
    let msg = err.to_string();
    assert!(msg.contains("缺列 exemplars.timestamp, env"), "{msg}");
    assert!(msg.contains("--ddl"), "要告诉人怎么补: {msg}");

    let queries = server.await.unwrap();
    assert_eq!(queries.len(), 3, "{queries:?}");
    assert!(
        queries[2].contains("system.columns")
            && queries[2].contains("database = 'logs'")
            && queries[2].contains("table = 'otel_metric'"),
        "{}",
        queries[2]
    );
}

#[tokio::test]
async fn healthcheck_passes_when_columns_present() {
    let columns: &'static str = Box::leak(
        format!(
            "{}env\tLowCardinality(String)\nextra_col\tString\n",
            columns_tsv(&[], &[])
        )
        .into_boxed_str(),
    );
    let (endpoint, server) = serve_sequence(vec!["1\n", "1\n", columns]).await;
    let sink = ClickhouseSink::new(endpoint, "logs", "otel_metric")
        .timeout(Duration::from_secs(5))
        .extra_columns(vec![(
            "env".to_owned(),
            "LowCardinality(String)".to_owned(),
        )]);

    sink.healthcheck().await.expect("列齐了不该报错");
    assert_eq!(server.await.unwrap().len(), 3);
}

/// 属性列手建成了 Map：列名都在，healthcheck 也要拦下来，并说明怎么迁。
#[tokio::test]
async fn healthcheck_rejects_map_typed_attribute_columns() {
    let columns = columns_tsv(
        &[],
        &[
            ("attributes", "Map(LowCardinality(String), String)"),
            (
                "exemplars.attributes",
                "Array(Map(LowCardinality(String), String))",
            ),
        ],
    );
    let (endpoint, _server) = serve_sequence(vec!["1\n", "1\n", columns]).await;
    let sink = ClickhouseSink::new(endpoint, "logs", "otel_metric").timeout(Duration::from_secs(5));

    let err = sink.healthcheck().await.expect_err("Map 列应当报错");
    let msg = err.to_string();
    assert!(
        msg.contains("attributes 是 Map(LowCardinality(String), String)"),
        "{msg}"
    );
    assert!(msg.contains("exemplars.attributes 是 Array(Map"), "{msg}");
    assert!(
        !msg.contains("resource_attributes"),
        "JSON 的列别点名: {msg}"
    );
    assert!(
        msg.contains("DROP") && msg.contains("--ddl"),
        "要告诉人怎么迁: {msg}"
    );
}

/// 配了路径提示、但表是早先建的（列类型还是光秃秃的 JSON）：提示只在建表时生效，
/// healthcheck 不该因此失败，但要 warn 出来，否则人不会知道白配了。
#[tokio::test]
async fn healthcheck_accepts_table_without_the_configured_hints() {
    let (endpoint, server) = serve_sequence(vec!["1\n", "1\n", columns_tsv(&[], &[])]).await;
    let sink = ClickhouseSink::new(endpoint, "logs", "otel_metric")
        .timeout(Duration::from_secs(5))
        .attribute_types(vec![("http.route".to_owned(), "String".to_owned())]);

    sink.healthcheck()
        .await
        .expect("提示没生效是 warn，不是错误");
    assert_eq!(server.await.unwrap().len(), 3);
}

/// 表上已经带着提示：一声不吭地过。
#[tokio::test]
async fn healthcheck_is_quiet_when_hints_are_applied() {
    let columns = columns_tsv(
        &[],
        &[
            (
                "attributes",
                "JSON(http.route String, SKIP REGEXP '^debug\\\\..*')",
            ),
            (
                "resource_attributes",
                "JSON(http.route String, SKIP REGEXP '^debug\\\\..*')",
            ),
        ],
    );
    let (endpoint, _server) = serve_sequence(vec!["1\n", "1\n", columns]).await;
    let sink = ClickhouseSink::new(endpoint, "logs", "otel_metric")
        .timeout(Duration::from_secs(5))
        .attribute_types(vec![("http.route".to_owned(), "String".to_owned())])
        .attribute_skip_regexp(vec![r"^debug\..*".to_owned()]);

    sink.healthcheck()
        .await
        .expect("带提示的列是合法的 JSON 类型");
}

#[tokio::test]
async fn healthcheck_reports_missing_table() {
    let (endpoint, _server) = serve_sequence(vec!["1\n", "0\n"]).await;
    let sink = ClickhouseSink::new(endpoint, "logs", "otel_metric").timeout(Duration::from_secs(5));
    let err = sink.healthcheck().await.expect_err("表不存在应当报错");
    assert!(err.to_string().contains("不存在"), "{err}");
}
