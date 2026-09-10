//! 把 OTLP 的 `ExportMetricsServiceRequest` 拆成一条条 [`MetricEvent`]。
//!
//! gRPC 和 HTTP 两个入口、stdin 的 JSON 行，解出来的都是同一个类型，都经过这里。
//!
//! 一个数据点一条记录：`ResourceMetrics × ScopeMetrics × Metric × DataPoint` 全部展开，
//! 上面几层的字段（service_name、指标名、单位……）用 `Arc` 共享，不逐条拷。

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as Any;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    exemplar, metric::Data, number_data_point, ExponentialHistogramDataPoint, HistogramDataPoint,
    Metric, NumberDataPoint, SummaryDataPoint,
};
use serde_json::Value as Json;

use crate::error::Result;
use crate::event::{Attributes, Exemplar, MetricEvent, MetricType, Quantile, Temporality};

/// resource 里没有 `service.name` 时的兜底，和 OTel SDK 自己的默认值一个写法。
pub const UNKNOWN_SERVICE: &str = "unknown_service";

/// 上面几层共享给每个数据点的东西。
#[derive(Clone)]
struct Context {
    metric_name: Arc<str>,
    metric_unit: Arc<str>,
    metric_description: Arc<str>,
    service_name: Arc<str>,
    scope_name: Arc<str>,
    scope_version: Arc<str>,
    resource_attributes: Arc<Attributes>,
    shared: Option<Arc<BTreeMap<String, Json>>>,
}

impl Context {
    /// 一个只填好共享字段的空数据点，剩下的由各类型自己填。
    fn event(&self, metric_type: MetricType) -> MetricEvent {
        MetricEvent {
            metric_name: Arc::clone(&self.metric_name),
            metric_type,
            metric_unit: Arc::clone(&self.metric_unit),
            metric_description: Arc::clone(&self.metric_description),
            service_name: Arc::clone(&self.service_name),
            scope_name: Arc::clone(&self.scope_name),
            scope_version: Arc::clone(&self.scope_version),
            resource_attributes: Arc::clone(&self.resource_attributes),
            shared: self.shared.clone(),
            ..Default::default()
        }
    }
}

/// 一次导出请求里的全部数据点。`shared` 是要挂到每条上的静态字段。
pub fn convert(
    request: ExportMetricsServiceRequest,
    shared: Option<&Arc<BTreeMap<String, Json>>>,
) -> Vec<MetricEvent> {
    let mut out = Vec::new();

    for resource_metrics in request.resource_metrics {
        let resource_attributes = Arc::new(
            resource_metrics
                .resource
                .map(|r| attributes(r.attributes))
                .unwrap_or_default(),
        );
        let service_name: Arc<str> = Arc::from(
            resource_attributes
                .get("service.name")
                .and_then(Json::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(UNKNOWN_SERVICE),
        );

        for scope_metrics in resource_metrics.scope_metrics {
            let (scope_name, scope_version): (Arc<str>, Arc<str>) = match scope_metrics.scope {
                Some(scope) => (Arc::from(scope.name), Arc::from(scope.version)),
                None => (Arc::from(""), Arc::from("")),
            };
            for metric in scope_metrics.metrics {
                let context = Context {
                    metric_name: Arc::from(metric.name.as_str()),
                    metric_unit: Arc::from(metric.unit.as_str()),
                    metric_description: Arc::from(metric.description.as_str()),
                    service_name: Arc::clone(&service_name),
                    scope_name: Arc::clone(&scope_name),
                    scope_version: Arc::clone(&scope_version),
                    resource_attributes: Arc::clone(&resource_attributes),
                    shared: shared.cloned(),
                };
                convert_metric(metric, &context, &mut out);
            }
        }
    }
    out
}

fn convert_metric(metric: Metric, context: &Context, out: &mut Vec<MetricEvent>) {
    // `data` 为空是「只有元信息、没有数据点」的指标，OTLP 允许，跳过就是
    match metric.data {
        Some(Data::Gauge(gauge)) => {
            out.reserve(gauge.data_points.len());
            for point in gauge.data_points {
                out.push(number_point(point, context, MetricType::Gauge));
            }
        }
        Some(Data::Sum(sum)) => {
            out.reserve(sum.data_points.len());
            let temporality = Temporality::from_otlp(sum.aggregation_temporality);
            for point in sum.data_points {
                let mut event = number_point(point, context, MetricType::Sum);
                event.temporality = temporality;
                event.is_monotonic = sum.is_monotonic;
                out.push(event);
            }
        }
        Some(Data::Histogram(histogram)) => {
            out.reserve(histogram.data_points.len());
            let temporality = Temporality::from_otlp(histogram.aggregation_temporality);
            for point in histogram.data_points {
                let mut event = histogram_point(point, context);
                event.temporality = temporality;
                out.push(event);
            }
        }
        Some(Data::ExponentialHistogram(histogram)) => {
            out.reserve(histogram.data_points.len());
            let temporality = Temporality::from_otlp(histogram.aggregation_temporality);
            for point in histogram.data_points {
                let mut event = exponential_histogram_point(point, context);
                event.temporality = temporality;
                out.push(event);
            }
        }
        Some(Data::Summary(summary)) => {
            out.reserve(summary.data_points.len());
            for point in summary.data_points {
                out.push(summary_point(point, context));
            }
        }
        None => {}
    }
}

fn number_point(point: NumberDataPoint, context: &Context, kind: MetricType) -> MetricEvent {
    let mut event = context.event(kind);
    event.timestamp = point.time_unix_nano;
    event.start_timestamp = point.start_time_unix_nano;
    event.attributes = attributes(point.attributes);
    event.value = match point.value {
        Some(number_data_point::Value::AsDouble(value)) => value,
        // 整数计数器也存进同一列。i64 超过 2^53 之后转 f64 会丢精度，
        // counter 要到这个量级才会碰上，先按一列装下的简单做法来。
        Some(number_data_point::Value::AsInt(value)) => value as f64,
        None => 0.0,
    };
    event.exemplars = exemplars(point.exemplars);
    event.flags = point.flags;
    event
}

fn histogram_point(point: HistogramDataPoint, context: &Context) -> MetricEvent {
    let mut event = context.event(MetricType::Histogram);
    event.timestamp = point.time_unix_nano;
    event.start_timestamp = point.start_time_unix_nano;
    event.attributes = attributes(point.attributes);
    event.count = point.count;
    event.sum = point.sum.unwrap_or_default();
    event.min = point.min.unwrap_or_default();
    event.max = point.max.unwrap_or_default();
    event.bucket_counts = point.bucket_counts;
    event.explicit_bounds = point.explicit_bounds;
    event.exemplars = exemplars(point.exemplars);
    event.flags = point.flags;
    event
}

fn exponential_histogram_point(
    point: ExponentialHistogramDataPoint,
    context: &Context,
) -> MetricEvent {
    let mut event = context.event(MetricType::ExponentialHistogram);
    event.timestamp = point.time_unix_nano;
    event.start_timestamp = point.start_time_unix_nano;
    event.attributes = attributes(point.attributes);
    event.count = point.count;
    event.sum = point.sum.unwrap_or_default();
    event.min = point.min.unwrap_or_default();
    event.max = point.max.unwrap_or_default();
    event.scale = point.scale;
    event.zero_count = point.zero_count;
    event.zero_threshold = point.zero_threshold;
    if let Some(positive) = point.positive {
        event.positive_offset = positive.offset;
        event.positive_bucket_counts = positive.bucket_counts;
    }
    if let Some(negative) = point.negative {
        event.negative_offset = negative.offset;
        event.negative_bucket_counts = negative.bucket_counts;
    }
    event.exemplars = exemplars(point.exemplars);
    event.flags = point.flags;
    event
}

fn summary_point(point: SummaryDataPoint, context: &Context) -> MetricEvent {
    let mut event = context.event(MetricType::Summary);
    event.timestamp = point.time_unix_nano;
    event.start_timestamp = point.start_time_unix_nano;
    event.attributes = attributes(point.attributes);
    event.count = point.count;
    event.sum = point.sum;
    event.quantiles = point
        .quantile_values
        .into_iter()
        .map(|q| Quantile {
            quantile: q.quantile,
            value: q.value,
        })
        .collect();
    event.flags = point.flags;
    event
}

fn exemplars(list: Vec<opentelemetry_proto::tonic::metrics::v1::Exemplar>) -> Vec<Exemplar> {
    list.into_iter()
        .map(|e| Exemplar {
            timestamp: e.time_unix_nano,
            value: match e.value {
                Some(exemplar::Value::AsDouble(value)) => value,
                Some(exemplar::Value::AsInt(value)) => value as f64,
                None => 0.0,
            },
            trace_id: hex(&e.trace_id),
            span_id: hex(&e.span_id),
            attributes: attributes(e.filtered_attributes),
        })
        .collect()
}

/// 原始 id 字节 → 小写 hex。OTLP 里 trace id 是 16 字节、span id 是 8 字节，所以出来
/// 就是 32 / 16 位，和 tracepipe / logpipe 落库的写法一样；空的给空串。
pub fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// 属性列表 → map。同名 key 后者覆盖前者（OTLP 规定 key 唯一，真重复了也别报错）。
pub fn attributes(list: Vec<KeyValue>) -> Attributes {
    list.into_iter()
        .map(|kv| (kv.key, attribute_value(kv.value)))
        .collect()
}

/// `AnyValue` → JSON，类型原样保留，落到 ClickHouse 的 `JSON` 列里每个 key 就是带类型的
/// 子列：字符串、整数、小数、布尔照搬；bytes 转 base64 串；数组、嵌套对象就是 JSON 数组 /
/// 对象。没有值的属性记 `null`。
pub fn attribute_value(value: Option<AnyValue>) -> Json {
    value.and_then(|v| v.value).map_or(Json::Null, to_json)
}

fn to_json(value: Any) -> Json {
    match value {
        Any::StringValue(s) => Json::from(s),
        Any::BoolValue(b) => Json::from(b),
        Any::IntValue(i) => Json::from(i),
        // NaN / Inf 在 JSON 里没有写法，记 null
        Any::DoubleValue(d) => serde_json::Number::from_f64(d).map_or(Json::Null, Json::Number),
        Any::BytesValue(b) => Json::from(base64::engine::general_purpose::STANDARD.encode(b)),
        Any::ArrayValue(array) => Json::Array(
            array
                .values
                .into_iter()
                .map(|v| v.value.map_or(Json::Null, to_json))
                .collect(),
        ),
        Any::KvlistValue(list) => Json::Object(
            list.values
                .into_iter()
                .map(|kv| {
                    (
                        kv.key,
                        kv.value.and_then(|v| v.value).map_or(Json::Null, to_json),
                    )
                })
                .collect(),
        ),
        // 只在 profiling 信号里出现，metric 里遇到按 OTLP 的说法当空值处理
        Any::StringValueStrindex(_) => Json::Null,
    }
}

/// 解析一行 OTLP/JSON（`ExportMetricsServiceRequest` 的 JSON 编码，也就是 OTel collector
/// `file` exporter 每行写的那种）。
pub fn decode_json(text: &str) -> Result<ExportMetricsServiceRequest> {
    decode_json_bytes(text.as_bytes())
}

/// 同上，直接吃字节（OTLP/HTTP 的 body）。
///
/// 先解成 `serde_json::Value` 补一道再交给 serde，慢一点但稳，原因见 [`normalize`]。
/// JSON 本来就是 OTLP 的次要编码（SDK 默认发 protobuf），这点开销换的是不丢数据。
pub fn decode_json_bytes(bytes: &[u8]) -> Result<ExportMetricsServiceRequest> {
    let mut value: Json = serde_json::from_slice(bytes)?;
    let expected = normalize(&mut value);
    let request: ExportMetricsServiceRequest = serde_json::from_value(value)?;

    // normalize 补的是**已知**的几处。将来 opentelemetry-proto 又多一处对不齐的，
    // 症状还是「metric 的 data 悄悄变成 None」，这里至少喊一声，不至于查半天。
    let decoded = count_points(&request);
    if decoded < expected {
        tracing::warn!(
            expected,
            decoded,
            "OTLP/JSON 里有数据点没能解出来（多半是 opentelemetry-proto 的 serde 和 \
             protobuf JSON 映射对不齐），这些点被丢了"
        );
    }
    Ok(request)
}

fn count_points(request: &ExportMetricsServiceRequest) -> usize {
    request
        .resource_metrics
        .iter()
        .flat_map(|r| r.scope_metrics.iter())
        .flat_map(|s| s.metrics.iter())
        .map(|metric| match &metric.data {
            Some(Data::Gauge(gauge)) => gauge.data_points.len(),
            Some(Data::Sum(sum)) => sum.data_points.len(),
            Some(Data::Histogram(histogram)) => histogram.data_points.len(),
            Some(Data::ExponentialHistogram(histogram)) => histogram.data_points.len(),
            Some(Data::Summary(summary)) => summary.data_points.len(),
            None => 0,
        })
        .sum()
}

/// OTLP 里数据点挂在 `Metric` 的哪个字段下（protobuf 的 oneof，JSON 里就是 key）。
const DATA_KEYS: [&str; 5] = [
    "gauge",
    "sum",
    "histogram",
    "exponentialHistogram",
    "summary",
];

/// 把 OTLP/JSON 补成 opentelemetry-proto 的 serde 认得的样子，返回 JSON 里一共有多少
/// 个数据点。
///
/// 为什么非补不可：`Metric.data` 是个 `#[serde(flatten)]` 的 oneof，**底下任何一处解不
/// 出来，整个 metric 的 data 会静默变成 `None`** —— 不报错，就是一整个指标凭空消失。
/// 已知对不齐的三处：
///
/// * 数据点的值 `asInt`：protobuf 的 JSON 映射规定 64 位整数编成字符串，而这个 oneof
///   上没挂「字符串 → i64」的 deserializer，一列整数指标会全变成 0（甚至整条没了）；
/// * `Exemplar` / `Buckets`（指数直方图的正负桶）/ `ValueAtQuantile`（Summary 的分位点）
///   这三个类型没有 `#[serde(default)]`，而 proto3 的 JSON 映射会省掉零值字段，
///   一省就是 `missing field`；
/// * exemplar 的值同样是个**没有** flatten 的 oneof，得包进 `value` 里。
///
/// protobuf 那条路（gRPC、`Content-Type: application/x-protobuf`）不经过这里，也没有
/// 这些问题。
fn normalize(request: &mut Json) -> usize {
    let mut points = 0;
    for resource in array_mut(request, "resourceMetrics") {
        for scope in array_mut(resource, "scopeMetrics") {
            for metric in array_mut(scope, "metrics") {
                for key in DATA_KEYS {
                    let Some(data) = metric.get_mut(key) else {
                        continue;
                    };
                    for point in array_mut(data, "dataPoints") {
                        normalize_point(point, key);
                        points += 1;
                    }
                }
            }
        }
    }
    points
}

fn array_mut<'a>(value: &'a mut Json, key: &str) -> impl Iterator<Item = &'a mut Json> {
    value
        .get_mut(key)
        .and_then(Json::as_array_mut)
        .into_iter()
        .flatten()
}

fn normalize_point(point: &mut Json, kind: &str) {
    let Some(point) = point.as_object_mut() else {
        return;
    };
    unstring_as_int(point);

    if let Some(exemplars) = point.get_mut("exemplars").and_then(Json::as_array_mut) {
        for exemplar in exemplars {
            normalize_exemplar(exemplar);
        }
    }

    match kind {
        "exponentialHistogram" => {
            // 这个数据点类型整个没有 `#[serde(default)]`，非 Option 的字段一个都不能少
            // （Option 的 sum / min / max / positive / negative 缺了没事）
            for (key, default) in [
                ("attributes", Json::Array(Vec::new())),
                ("startTimeUnixNano", Json::from("0")),
                ("timeUnixNano", Json::from("0")),
                ("count", Json::from("0")),
                ("scale", Json::from(0)),
                ("zeroCount", Json::from("0")),
                ("zeroThreshold", Json::from(0.0)),
                ("flags", Json::from(0)),
                ("exemplars", Json::Array(Vec::new())),
            ] {
                point.entry(key).or_insert(default);
            }
            for side in ["positive", "negative"] {
                let Some(buckets) = point.get_mut(side).and_then(Json::as_object_mut) else {
                    continue;
                };
                buckets.entry("offset").or_insert_with(|| Json::from(0));
                buckets
                    .entry("bucketCounts")
                    .or_insert_with(|| Json::Array(Vec::new()));
            }
        }
        "summary" => {
            for quantile in point
                .get_mut("quantileValues")
                .and_then(Json::as_array_mut)
                .into_iter()
                .flatten()
            {
                let Some(quantile) = quantile.as_object_mut() else {
                    continue;
                };
                quantile
                    .entry("quantile")
                    .or_insert_with(|| Json::from(0.0));
                quantile.entry("value").or_insert_with(|| Json::from(0.0));
            }
        }
        _ => {}
    }
}

fn normalize_exemplar(exemplar: &mut Json) {
    let Some(exemplar) = exemplar.as_object_mut() else {
        return;
    };
    unstring_as_int(exemplar);
    // 值要从平铺的 asDouble / asInt 收进 value 里（这个 oneof 没有 flatten）
    if !exemplar.contains_key("value") {
        if let Some((key, number)) = ["asDouble", "asInt"]
            .into_iter()
            .find_map(|key| exemplar.remove_entry(key))
        {
            exemplar.insert(
                "value".to_owned(),
                Json::Object([(key, number)].into_iter().collect()),
            );
        }
    }
    // 非 Option 的字段缺一个就是 missing field（value 是 Option，缺了没事）
    for (key, default) in [
        ("filteredAttributes", Json::Array(Vec::new())),
        ("timeUnixNano", Json::from("0")),
        ("spanId", Json::from("")),
        ("traceId", Json::from("")),
    ] {
        exemplar.entry(key).or_insert(default);
    }
}

/// `"asInt": "123"` → `"asInt": 123`。
fn unstring_as_int(map: &mut serde_json::Map<String, Json>) {
    if let Some(Json::String(raw)) = map.get("asInt") {
        if let Ok(number) = raw.parse::<i64>() {
            map.insert("asInt".to_owned(), Json::from(number));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};
    use opentelemetry_proto::tonic::metrics::v1::{
        exponential_histogram_data_point::Buckets, summary_data_point::ValueAtQuantile,
        ExponentialHistogram, Gauge, Histogram, ResourceMetrics, ScopeMetrics, Summary,
    };

    fn kv(key: &str, value: Any) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(AnyValue { value: Some(value) }),
            ..Default::default()
        }
    }

    /// 只有一个指标的一次导出请求。
    fn request(metric: Metric) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![metric],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[test]
    fn hex_is_lowercase_and_empty_for_missing() {
        assert_eq!(hex(&[0xe8, 0x9a, 0x47, 0x68]), "e89a4768");
        assert_eq!(hex(&[]), "");
        assert_eq!(hex(&[0u8; 16]).len(), 32);
    }

    #[test]
    fn attribute_values_keep_their_types() {
        let attrs = attributes(vec![
            kv("s", Any::StringValue("x".into())),
            kv("b", Any::BoolValue(true)),
            kv("i", Any::IntValue(-7)),
            kv("d", Any::DoubleValue(1.5)),
            kv("bytes", Any::BytesValue(vec![0xde, 0xad])),
            kv(
                "arr",
                Any::ArrayValue(ArrayValue {
                    values: vec![
                        AnyValue {
                            value: Some(Any::IntValue(1)),
                        },
                        AnyValue {
                            value: Some(Any::StringValue("two".into())),
                        },
                    ],
                }),
            ),
            kv(
                "obj",
                Any::KvlistValue(KeyValueList {
                    values: vec![kv("k", Any::BoolValue(false))],
                }),
            ),
            KeyValue {
                key: "empty".into(),
                value: None,
                ..Default::default()
            },
        ]);
        assert_eq!(attrs["s"], Json::from("x"));
        assert_eq!(attrs["b"], Json::from(true));
        assert_eq!(attrs["i"], Json::from(-7));
        assert_eq!(attrs["d"], Json::from(1.5));
        assert_eq!(attrs["bytes"], Json::from("3q0="));
        assert_eq!(attrs["arr"], serde_json::json!([1, "two"]));
        assert_eq!(attrs["obj"], serde_json::json!({"k": false}));
        assert_eq!(attrs["empty"], Json::Null);
    }

    #[test]
    fn sum_keeps_temporality_and_monotonicity() {
        let events = convert(
            request(Metric {
                name: "http.server.request.count".into(),
                unit: "1".into(),
                data: Some(Data::Sum(opentelemetry_proto::tonic::metrics::v1::Sum {
                    data_points: vec![NumberDataPoint {
                        time_unix_nano: 1_789_000_000_000_000_000,
                        start_time_unix_nano: 1_788_000_000_000_000_000,
                        value: Some(number_data_point::Value::AsInt(12)),
                        attributes: vec![kv("http.route", Any::StringValue("/orders".into()))],
                        ..Default::default()
                    }],
                    aggregation_temporality: 2,
                    is_monotonic: true,
                })),
                ..Default::default()
            }),
            None,
        );

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(&*event.metric_name, "http.server.request.count");
        assert_eq!(event.metric_type, MetricType::Sum);
        assert_eq!(event.temporality, Temporality::Cumulative);
        assert!(event.is_monotonic);
        assert_eq!(event.value, 12.0);
        assert_eq!(event.timestamp, 1_789_000_000_000_000_000);
        assert_eq!(event.start_timestamp, 1_788_000_000_000_000_000);
        assert_eq!(&*event.service_name, UNKNOWN_SERVICE);
        assert_eq!(event.attribute("http.route"), Some(&Json::from("/orders")));
    }

    #[test]
    fn histogram_keeps_buckets_and_exemplars() {
        let events = convert(
            request(Metric {
                name: "http.server.request.duration".into(),
                data: Some(Data::Histogram(Histogram {
                    data_points: vec![HistogramDataPoint {
                        time_unix_nano: 10,
                        count: 7,
                        sum: Some(123.5),
                        min: Some(1.0),
                        max: Some(90.0),
                        bucket_counts: vec![3, 3, 1],
                        explicit_bounds: vec![5.0, 50.0],
                        exemplars: vec![opentelemetry_proto::tonic::metrics::v1::Exemplar {
                            time_unix_nano: 9,
                            value: Some(exemplar::Value::AsDouble(90.0)),
                            trace_id: vec![0xe8; 16],
                            span_id: vec![0x0a; 8],
                            filtered_attributes: vec![kv("pod", Any::StringValue("p-1".into()))],
                        }],
                        ..Default::default()
                    }],
                    aggregation_temporality: 1,
                })),
                ..Default::default()
            }),
            None,
        );

        let event = &events[0];
        assert_eq!(event.metric_type, MetricType::Histogram);
        assert_eq!(event.temporality, Temporality::Delta);
        assert_eq!(event.count, 7);
        assert_eq!(event.sum, 123.5);
        assert_eq!(event.min, 1.0);
        assert_eq!(event.max, 90.0);
        assert_eq!(event.bucket_counts, [3, 3, 1]);
        assert_eq!(event.explicit_bounds, [5.0, 50.0]);
        assert_eq!(event.exemplars.len(), 1);
        assert_eq!(event.exemplars[0].trace_id.len(), 32);
        assert_eq!(event.exemplars[0].span_id, "0a0a0a0a0a0a0a0a");
        assert_eq!(event.exemplars[0].value, 90.0);
        assert_eq!(event.exemplars[0].attributes["pod"], Json::from("p-1"));
        // 没上报的 sum 是 0，不是 null
        assert_eq!(event.value, 0.0);
    }

    #[test]
    fn exponential_histogram_and_summary() {
        let events = convert(
            request(Metric {
                name: "rpc.duration".into(),
                data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                    data_points: vec![ExponentialHistogramDataPoint {
                        time_unix_nano: 10,
                        count: 5,
                        sum: Some(9.0),
                        scale: 3,
                        zero_count: 1,
                        zero_threshold: 1e-9,
                        positive: Some(Buckets {
                            offset: 2,
                            bucket_counts: vec![1, 2],
                        }),
                        negative: Some(Buckets {
                            offset: -1,
                            bucket_counts: vec![1],
                        }),
                        ..Default::default()
                    }],
                    aggregation_temporality: 2,
                })),
                ..Default::default()
            }),
            None,
        );
        let event = &events[0];
        assert_eq!(event.metric_type, MetricType::ExponentialHistogram);
        assert_eq!(event.scale, 3);
        assert_eq!(event.zero_count, 1);
        assert_eq!(event.zero_threshold, 1e-9);
        assert_eq!(event.positive_offset, 2);
        assert_eq!(event.positive_bucket_counts, [1, 2]);
        assert_eq!(event.negative_offset, -1);
        assert_eq!(event.negative_bucket_counts, [1]);

        let events = convert(
            request(Metric {
                name: "jvm.gc.duration".into(),
                data: Some(Data::Summary(Summary {
                    data_points: vec![SummaryDataPoint {
                        time_unix_nano: 10,
                        count: 3,
                        sum: 6.0,
                        quantile_values: vec![
                            ValueAtQuantile {
                                quantile: 0.5,
                                value: 1.5,
                            },
                            ValueAtQuantile {
                                quantile: 0.99,
                                value: 4.0,
                            },
                        ],
                        ..Default::default()
                    }],
                })),
                ..Default::default()
            }),
            None,
        );
        let event = &events[0];
        assert_eq!(event.metric_type, MetricType::Summary);
        assert_eq!(event.temporality, Temporality::Unspecified);
        assert_eq!(event.quantiles.len(), 2);
        assert_eq!(event.quantiles[1].quantile, 0.99);
        assert_eq!(event.quantiles[1].value, 4.0);
    }

    #[test]
    fn decodes_otlp_json_with_string_encoded_nanos() {
        // OTLP/JSON 的口径：fixed64 是十进制串、枚举是数字
        let request = decode_json(
            r#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"order-service"}}]},"scopeMetrics":[{"scope":{"name":"io.opentelemetry.runtime-telemetry","version":"2.9.0"},"metrics":[{"name":"jvm.memory.used","unit":"By","description":"Measure of memory used","gauge":{"dataPoints":[{"timeUnixNano":"1789000000000000000","asInt":"123456","attributes":[{"key":"jvm.memory.type","value":{"stringValue":"heap"}}]}]}}]}]}]}"#,
        )
        .unwrap();
        let events = convert(request, None);

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(&*event.service_name, "order-service");
        assert_eq!(&*event.scope_name, "io.opentelemetry.runtime-telemetry");
        assert_eq!(&*event.metric_name, "jvm.memory.used");
        assert_eq!(&*event.metric_unit, "By");
        assert_eq!(&*event.metric_description, "Measure of memory used");
        assert_eq!(event.metric_type, MetricType::Gauge);
        assert_eq!(event.value, 123_456.0);
        assert_eq!(event.timestamp, 1_789_000_000_000_000_000);
        assert_eq!(
            event.attribute("jvm.memory.type"),
            Some(&Json::from("heap"))
        );
        assert_eq!(
            event.attribute("service.name"),
            Some(&Json::from("order-service"))
        );
    }

    /// 真实发送方写出来的 OTLP/JSON：64 位整数是字符串、零值字段被省掉、exemplar 的值
    /// 平铺在外面。这些都会让 opentelemetry-proto 的 serde **静默**丢掉整个指标，
    /// 所以这条用例是 JSON 那条路的护栏，五种类型一次全过。
    #[test]
    fn realistic_otlp_json_keeps_every_data_point() {
        let request = decode_json(
            r#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"order-service"}}]},"scopeMetrics":[{"scope":{"name":"app"},"metrics":[
                {"name":"g","gauge":{"dataPoints":[{"timeUnixNano":"10","asInt":"123456"}]}},
                {"name":"s","sum":{"aggregationTemporality":2,"isMonotonic":true,"dataPoints":[{"timeUnixNano":"10","asInt":"12"}]}},
                {"name":"h","histogram":{"aggregationTemporality":1,"dataPoints":[{"timeUnixNano":"10","count":"7","sum":123.5,"bucketCounts":["3","3","1"],"explicitBounds":[5,50],"exemplars":[{"timeUnixNano":"9","asDouble":90,"traceId":"e89a476882236ce0f1186d1522c8f59f","spanId":"e8b0e73e2132f21c"}]}]}},
                {"name":"e","exponentialHistogram":{"aggregationTemporality":2,"dataPoints":[{"timeUnixNano":"10","count":"5","scale":3,"positive":{"bucketCounts":["1","2"]},"negative":{"offset":-1,"bucketCounts":["1"]},"exemplars":[{"timeUnixNano":"9","asInt":"90"}]}]}},
                {"name":"q","summary":{"dataPoints":[{"timeUnixNano":"10","count":"3","sum":6.0,"quantileValues":[{"value":1.5},{"quantile":0.99,"value":4.0}]}]}}
            ]}]}]}"#,
        )
        .unwrap();
        let events = convert(request, None);

        let names: Vec<&str> = events.iter().map(|e| &*e.metric_name).collect();
        assert_eq!(names, ["g", "s", "h", "e", "q"], "有指标被静默丢了");

        // 整数值：字符串编码的 asInt 不能变成 0
        assert_eq!(events[0].value, 123_456.0);
        assert_eq!(events[1].value, 12.0);
        assert!(events[1].is_monotonic);

        // exemplar：值是平铺的，其余字段被省掉
        assert_eq!(events[2].exemplars.len(), 1);
        assert_eq!(events[2].exemplars[0].value, 90.0);
        assert_eq!(
            events[2].exemplars[0].trace_id,
            "e89a476882236ce0f1186d1522c8f59f"
        );
        assert_eq!(
            events[3].exemplars[0].value, 90.0,
            "exemplar 的 asInt 也是串"
        );
        assert!(events[3].exemplars[0].trace_id.is_empty());

        // 指数桶：positive 省了 offset（零值），negative 省了什么都没省
        assert_eq!(events[3].positive_offset, 0);
        assert_eq!(events[3].positive_bucket_counts, [1, 2]);
        assert_eq!(events[3].negative_offset, -1);

        // 分位点：第一个省了 quantile（0.0）
        assert_eq!(events[4].quantiles.len(), 2);
        assert_eq!(events[4].quantiles[0].quantile, 0.0);
        assert_eq!(events[4].quantiles[0].value, 1.5);
        assert_eq!(events[4].quantiles[1].quantile, 0.99);
    }

    #[test]
    fn resource_and_metric_level_fields_are_shared() {
        let mut metric = Metric {
            name: "system.cpu.time".into(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint::default(), NumberDataPoint::default()],
            })),
            ..Default::default()
        };
        metric.description = "cpu".into();
        let events = convert(request(metric), None);

        assert_eq!(events.len(), 2);
        assert!(Arc::ptr_eq(
            &events[0].resource_attributes,
            &events[1].resource_attributes
        ));
        assert!(Arc::ptr_eq(&events[0].metric_name, &events[1].metric_name));
    }

    /// 没有数据点、或者 `data` 整个为空的指标，跳过而不是造一条空记录。
    #[test]
    fn metric_without_data_points_is_skipped() {
        assert!(convert(
            request(Metric {
                name: "empty".into(),
                data: None,
                ..Default::default()
            }),
            None
        )
        .is_empty());
        assert!(convert(
            request(Metric {
                name: "empty".into(),
                data: Some(Data::Gauge(Gauge {
                    data_points: Vec::new()
                })),
                ..Default::default()
            }),
            None
        )
        .is_empty());
    }
}
