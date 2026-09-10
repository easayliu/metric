//! 端到端：OTLP 客户端 -> 接收端 -> 攒批 -> 入库。

use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use metricpipe::batch::{BatchConfig, RetryConfig};
use metricpipe::pipeline::OnError;
use metricpipe::sink::{MemorySink, Sink};
use metricpipe::source::OtlpSource;
use metricpipe::{MetricEvent, MetricType, Pipeline, Temporality};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as Any;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric::Data, number_data_point, Exemplar, Gauge, Histogram, HistogramDataPoint, Metric,
    NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

const TRACE_ID: [u8; 16] = [
    0xe8, 0x9a, 0x47, 0x68, 0x82, 0x23, 0x6c, 0xe0, 0xf1, 0x18, 0x6d, 0x15, 0x22, 0xc8, 0xf5, 0x9f,
];
const SPAN_ID: [u8; 8] = [0xe8, 0xb0, 0xe7, 0x3e, 0x21, 0x32, 0xf2, 0x1c];
/// 2026-09-07 03:04:08.914293456 UTC
const NOW: u64 = 1_788_750_248_914_293_456;
const MINUTE: u64 = 60_000_000_000;

fn kv(key: &str, value: Any) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

/// 一个 resource、一个 scope、三个指标：一个 Sum（整数 counter）、一个 Histogram
/// （带 exemplar）、一个 Gauge。总共四个数据点（Gauge 有两条时间线）。
fn sample_request() -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![
                    kv("service.name", Any::StringValue("order-service".into())),
                    kv(
                        "k8s.pod.name",
                        Any::StringValue("order-service-7d9f8b6c4-abcde".into()),
                    ),
                ],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "io.opentelemetry.tomcat-10.0".into(),
                    version: "2.9.0".into(),
                    ..Default::default()
                }),
                metrics: vec![
                    Metric {
                        name: "http.server.request.count".into(),
                        unit: "1".into(),
                        description: "请求数".into(),
                        data: Some(Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                start_time_unix_nano: NOW - MINUTE,
                                time_unix_nano: NOW,
                                value: Some(number_data_point::Value::AsInt(12)),
                                attributes: vec![kv(
                                    "http.route",
                                    Any::StringValue("/orders/{id}".into()),
                                )],
                                ..Default::default()
                            }],
                            aggregation_temporality: 2,
                            is_monotonic: true,
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "http.server.request.duration".into(),
                        unit: "ms".into(),
                        data: Some(Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                start_time_unix_nano: NOW - MINUTE,
                                time_unix_nano: NOW,
                                count: 7,
                                sum: Some(123.5),
                                min: Some(1.0),
                                max: Some(90.0),
                                bucket_counts: vec![3, 3, 1],
                                explicit_bounds: vec![5.0, 50.0],
                                exemplars: vec![Exemplar {
                                    time_unix_nano: NOW - 1_000,
                                    value: Some(
                                        opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(90.0),
                                    ),
                                    trace_id: TRACE_ID.to_vec(),
                                    span_id: SPAN_ID.to_vec(),
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }],
                            aggregation_temporality: 1,
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "jvm.memory.used".into(),
                        unit: "By".into(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![
                                NumberDataPoint {
                                    time_unix_nano: NOW,
                                    value: Some(number_data_point::Value::AsInt(123_456)),
                                    attributes: vec![kv(
                                        "jvm.memory.type",
                                        Any::StringValue("heap".into()),
                                    )],
                                    ..Default::default()
                                },
                                NumberDataPoint {
                                    time_unix_nano: NOW,
                                    value: Some(number_data_point::Value::AsDouble(2048.5)),
                                    attributes: vec![kv(
                                        "jvm.memory.type",
                                        Any::StringValue("non_heap".into()),
                                    )],
                                    ..Default::default()
                                },
                            ],
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// 两个端口都用 0 绑好，交给 source，返回 (source, grpc 地址, http 地址)。
fn bound_source() -> (OtlpSource, String, String) {
    let grpc = TcpListener::bind("127.0.0.1:0").unwrap();
    let http = TcpListener::bind("127.0.0.1:0").unwrap();
    let grpc_addr = grpc.local_addr().unwrap().to_string();
    let http_addr = http.local_addr().unwrap().to_string();
    (
        OtlpSource::new().grpc_listener(grpc).http_listener(http),
        grpc_addr,
        http_addr,
    )
}

fn batch() -> BatchConfig {
    BatchConfig::default()
        .max_events(100)
        .timeout(Duration::from_millis(50))
}

async fn wait_for(mut ready: impl FnMut() -> bool, label: &str) {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        if ready() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("等待超时: {label}");
}

fn assert_sample(events: &[MetricEvent]) {
    assert_eq!(events.len(), 4, "三个指标一共四个数据点");

    let counter = &events[0];
    assert_eq!(&*counter.metric_name, "http.server.request.count");
    assert_eq!(counter.metric_type, MetricType::Sum);
    assert_eq!(counter.temporality, Temporality::Cumulative);
    assert!(counter.is_monotonic);
    assert_eq!(counter.value, 12.0);
    assert_eq!(counter.timestamp, NOW);
    assert_eq!(counter.start_timestamp, NOW - MINUTE);
    assert_eq!(&*counter.service_name, "order-service");
    assert_eq!(&*counter.scope_name, "io.opentelemetry.tomcat-10.0");
    assert_eq!(&*counter.metric_unit, "1");
    assert_eq!(
        counter.attribute("http.route"),
        Some(&serde_json::json!("/orders/{id}"))
    );
    assert_eq!(
        counter.attribute("k8s.pod.name"),
        Some(&serde_json::json!("order-service-7d9f8b6c4-abcde"))
    );

    let histogram = &events[1];
    assert_eq!(histogram.metric_type, MetricType::Histogram);
    assert_eq!(histogram.temporality, Temporality::Delta);
    assert_eq!(histogram.count, 7);
    assert_eq!(histogram.sum, 123.5);
    assert_eq!(histogram.bucket_counts, [3, 3, 1]);
    assert_eq!(histogram.explicit_bounds, [5.0, 50.0]);
    assert_eq!(histogram.exemplars.len(), 1);
    // exemplar 的 trace_id 和 tracepipe / logpipe 落库的写法一致，可以直接对上
    assert_eq!(
        histogram.exemplars[0].trace_id,
        "e89a476882236ce0f1186d1522c8f59f"
    );
    assert_eq!(histogram.exemplars[0].span_id, "e8b0e73e2132f21c");

    let heap = &events[2];
    assert_eq!(heap.metric_type, MetricType::Gauge);
    assert_eq!(heap.value, 123_456.0);
    assert_eq!(heap.temporality, Temporality::Unspecified);
    assert_eq!(events[3].value, 2048.5);

    // 同一个 resource / 同一个指标下的数据点共享同一份属性和指标名
    assert!(Arc::ptr_eq(
        &counter.resource_attributes,
        &histogram.resource_attributes
    ));
    assert!(Arc::ptr_eq(&heap.metric_name, &events[3].metric_name));
}

#[tokio::test]
async fn grpc_export_lands_in_sink() {
    let (source, grpc_addr, _) = bound_source();
    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source)
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();

    let mut client = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    let response = client.export(sample_request()).await.unwrap().into_inner();
    assert!(response.partial_success.is_none());

    wait_for(|| events.lock().unwrap().len() == 4, "四个数据点").await;
    running.stop().await.unwrap();
    assert_sample(&events.lock().unwrap());
}

#[tokio::test]
async fn http_protobuf_and_json_land_in_sink() {
    let (source, _, http_addr) = bound_source();
    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source)
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    let url = format!("http://{http_addr}/v1/metrics");
    let http = reqwest::Client::new();

    // protobuf
    let response = http
        .post(&url)
        .header("content-type", "application/x-protobuf")
        .body(sample_request().encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    wait_for(|| events.lock().unwrap().len() == 4, "protobuf 四个").await;
    assert_sample(&events.lock().unwrap());

    // JSON：OTLP/JSON 的写法，纳秒和 64 位整数都是十进制串
    let json = serde_json::to_string(&sample_request()).unwrap();
    assert!(
        json.contains("\"timeUnixNano\":\"1788750248914293456\""),
        "{json}"
    );
    let response = http
        .post(&url)
        .header("content-type", "application/json")
        .body(json)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "{}");
    wait_for(|| events.lock().unwrap().len() == 8, "JSON 再四个").await;
    assert_sample(&events.lock().unwrap()[4..]);

    running.stop().await.unwrap();
}

/// OTLP/JSON 里 64 位整数按 protobuf 的 JSON 映射编成字符串。数据点的值是个
/// flatten 的 oneof，serde 那层不认字符串，不修一道就会**静默**变成 0。
#[tokio::test]
async fn json_int_values_are_not_silently_dropped() {
    let (source, _, http_addr) = bound_source();
    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source)
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();

    let body = r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"jvm.memory.used","gauge":{"dataPoints":[{"timeUnixNano":"1788750248914293456","asInt":"123456"}]}}]}]}]}"#;
    let response = reqwest::Client::new()
        .post(format!("http://{http_addr}/v1/metrics"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    wait_for(|| events.lock().unwrap().len() == 1, "一个数据点").await;
    running.stop().await.unwrap();
    assert_eq!(events.lock().unwrap()[0].value, 123_456.0);
}

#[tokio::test]
async fn http_accepts_gzip_and_rejects_bad_input() {
    use std::io::Write;

    let (source, _, http_addr) = bound_source();
    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source)
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();
    let url = format!("http://{http_addr}/v1/metrics");
    let http = reqwest::Client::new();

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder
        .write_all(&sample_request().encode_to_vec())
        .unwrap();
    let response = http
        .post(&url)
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "gzip")
        .body(encoder.finish().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    wait_for(|| events.lock().unwrap().len() == 4, "gzip 四个").await;

    // 不认的 Content-Type
    let response = http.post(&url).body("x").send().await.unwrap();
    assert_eq!(response.status(), 415);
    // 解析不了
    let response = http
        .post(&url)
        .header("content-type", "application/json")
        .body("{not json")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    // 声明了 gzip 却不是
    let response = http
        .post(&url)
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "gzip")
        .body("plain")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);

    running.stop().await.unwrap();
    assert_eq!(events.lock().unwrap().len(), 4, "坏请求不该进来");
}

#[tokio::test]
async fn static_fields_and_transform() {
    let (source, grpc_addr, _) = bound_source();
    let fields = [("cluster".to_owned(), serde_json::Value::from("bj-prod"))]
        .into_iter()
        .collect();
    let sink = MemorySink::new();
    let events = sink.events();
    let running = Pipeline::builder()
        .source(source.fields(fields))
        // 只留 histogram，其余丢掉
        .transform(|mut event: MetricEvent| {
            (event.metric_type == MetricType::Histogram).then(|| {
                event.insert("env", "prod");
                event
            })
        })
        .sink(sink)
        .batch(batch())
        .build()
        .unwrap()
        .spawn();

    let mut client = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    client.export(sample_request()).await.unwrap();

    wait_for(|| events.lock().unwrap().len() == 1, "只留 Histogram").await;
    running.stop().await.unwrap();

    let events = events.lock().unwrap();
    assert_eq!(events[0].metric_type, MetricType::Histogram);
    assert!(
        events[0].fields.len() == 1,
        "静态字段走 shared，不逐条拷进 fields"
    );
    let json: serde_json::Value = serde_json::from_str(&events[0].to_json_line().unwrap()).unwrap();
    assert_eq!(json["cluster"], "bj-prod");
    assert_eq!(json["env"], "prod");
    assert_eq!(json["timestamp"], "2026-09-07 03:04:08.914293456+00:00");
}

struct FailingSink;

#[async_trait]
impl Sink for FailingSink {
    async fn write(&mut self, _events: &[MetricEvent]) -> metricpipe::Result<()> {
        Err(metricpipe::Error::other("存储挂了"))
    }
}

/// 没开 wait_for_write：客户端立刻拿到成功，写失败把 pipeline 停掉，根因不能被转述盖掉。
#[tokio::test]
async fn write_failure_stops_pipeline_and_reports_root_cause() {
    let (source, grpc_addr, _) = bound_source();
    let running = Pipeline::builder()
        .source(source)
        .sink(FailingSink)
        .batch(batch())
        .retry(RetryConfig {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(20),
        })
        .on_error(OnError::Stop)
        .build()
        .unwrap()
        .spawn();

    let mut client = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    client.export(sample_request()).await.unwrap();

    let err = running
        .wait()
        .await
        .expect_err("写入失败应当把 pipeline 停掉");
    assert!(
        err.to_string().contains("存储挂了"),
        "根因被转述盖掉了: {err}"
    );
}

/// 开了 wait_for_write：写失败要反映成客户端的导出失败（UNAVAILABLE），SDK 才会重发。
#[tokio::test]
async fn wait_for_write_surfaces_failure_to_client() {
    let (source, grpc_addr, _) = bound_source();
    let running = Pipeline::builder()
        .source(source.wait_for_write(true))
        .sink(FailingSink)
        .batch(batch())
        .retry(RetryConfig {
            max_attempts: 1,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        })
        .on_error(OnError::Drop)
        .build()
        .unwrap()
        .spawn();

    let mut client = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    let status = client
        .export(sample_request())
        .await
        .expect_err("没落库不该回成功");
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status}");

    running.stop().await.unwrap();
}

struct CountingSink(Arc<Mutex<usize>>);

#[async_trait]
impl Sink for CountingSink {
    async fn write(&mut self, events: &[MetricEvent]) -> metricpipe::Result<()> {
        *self.0.lock().unwrap() += events.len();
        Ok(())
    }
}

/// 开了 wait_for_write：回成功的时候数据已经在存储里了。
#[tokio::test]
async fn wait_for_write_acks_after_write() {
    let (source, _, http_addr) = bound_source();
    let written = Arc::new(Mutex::new(0usize));
    let running = Pipeline::builder()
        .source(source.wait_for_write(true))
        .sink(CountingSink(Arc::clone(&written)))
        .batch(batch())
        .build()
        .unwrap()
        .spawn();

    let response = reqwest::Client::new()
        .post(format!("http://{http_addr}/v1/metrics"))
        .header("content-type", "application/x-protobuf")
        .body(sample_request().encode_to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(*written.lock().unwrap(), 4, "回 200 之前就该写完");

    running.stop().await.unwrap();
}

struct SlowSink {
    writes: Arc<Mutex<usize>>,
    delay: Duration,
}

#[async_trait]
impl Sink for SlowSink {
    async fn write(&mut self, _events: &[MetricEvent]) -> metricpipe::Result<()> {
        tokio::time::sleep(self.delay).await;
        *self.writes.lock().unwrap() += 1;
        Ok(())
    }
}

/// 攒下一批要和写上一批重叠，而不是「攒满 -> 停下来写 -> 再从头攒」串成一条。
#[tokio::test]
async fn accumulation_overlaps_with_slow_writes() {
    const STEP: Duration = Duration::from_millis(120);
    const WINDOW: Duration = Duration::from_millis(960);

    let (source, grpc_addr, _) = bound_source();
    let writes = Arc::new(Mutex::new(0usize));
    let running = Pipeline::builder()
        .source(source)
        .sink(SlowSink {
            writes: Arc::clone(&writes),
            delay: STEP,
        })
        .batch(BatchConfig::default().max_events(1_000_000).timeout(STEP))
        .build()
        .unwrap()
        .spawn();

    let mut client = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .unwrap();
    let start = std::time::Instant::now();
    while start.elapsed() < WINDOW {
        client.export(sample_request()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    running.stop().await.unwrap();

    let count = *writes.lock().unwrap();
    let serial = WINDOW.as_millis() / (STEP.as_millis() * 2);
    assert!(
        count as u128 > serial + 1,
        "攒批与写入没有重叠：{WINDOW:?} 内只写了 {count} 批，串行也能到 {serial} 批"
    );
}

/// 端口被占着就整个不启动，报错里要有端口。
#[tokio::test]
async fn port_in_use_fails_fast() {
    let taken = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = taken.local_addr().unwrap();
    let err = Pipeline::builder()
        .source(OtlpSource::new().grpc(addr).no_http())
        .sink(MemorySink::new())
        .build()
        .unwrap()
        .spawn()
        .wait()
        .await
        .expect_err("端口被占应当启动失败");
    assert!(err.to_string().contains(&addr.port().to_string()), "{err}");
}
