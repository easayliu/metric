//! 指标事件：一个 OTLP 数据点拍平成一行记录，字段与 ClickHouse 表一一对应。
//!
//! ```text
//! ResourceMetrics ─┬─ resource.attributes ──▶ resource_attributes / service_name
//!                  └─ ScopeMetrics ─┬─ scope ──▶ scope_name / scope_version
//!                                   └─ Metric ─┬─ name / unit / description
//!                                              └─ DataPoint ──▶ 其余所有列（一个数据点一行）
//! ```
//!
//! 五种类型（Gauge / Sum / Histogram / ExponentialHistogram / Summary）落在**同一张表**
//! 里，用 `metric_type` 区分，各自只填得上的那几列（见 [`MetricEvent`] 各字段的说明）。
//! 分表当然更省空间，但一条 pipeline 只有一个 sink、一张表，混在一起也才好一次查完
//! ——「这个服务所有指标」不用 union 五张表。
//!
//! 时间戳是 UNIX 纳秒（OTLP 的口径），落库时按 sink 配置的时区换成墙上时间并带上偏移，
//! 见 [`format_timestamp`]。

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Offset, Timelike, Utc};
use chrono_tz::Tz;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::Value;

/// 属性保留 OTLP 里的类型：ClickHouse 那边是 `JSON` 列，每个 key 是一个带类型的子列。
/// 字符串、整数、小数、布尔原样；bytes 转 base64 串；数组和嵌套对象就是 JSON 数组 / 对象。
pub type Attributes = BTreeMap<String, Value>;

/// 数据点属于哪种指标。名字沿用 OTel collector clickhouse exporter 分表时的叫法
/// （`gauge` / `sum` / `histogram` ……），只是这里合成一列。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MetricType {
    #[default]
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
}

impl MetricType {
    pub fn as_str(self) -> &'static str {
        match self {
            MetricType::Gauge => "Gauge",
            MetricType::Sum => "Sum",
            MetricType::Histogram => "Histogram",
            MetricType::ExponentialHistogram => "ExponentialHistogram",
            MetricType::Summary => "Summary",
        }
    }

    /// 这种类型看的是 [`MetricEvent::value`]（而不是 count / sum / 桶）。
    pub fn is_scalar(self) -> bool {
        matches!(self, MetricType::Gauge | MetricType::Sum)
    }
}

/// OTLP 的 `AggregationTemporality`：这个值是「从 start_timestamp 累计到现在」还是
/// 「这一个上报周期内的增量」。算 rate 时区别很大，所以存下来。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Temporality {
    #[default]
    Unspecified,
    Delta,
    Cumulative,
}

impl Temporality {
    pub fn from_otlp(raw: i32) -> Self {
        match raw {
            1 => Temporality::Delta,
            2 => Temporality::Cumulative,
            _ => Temporality::Unspecified,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Temporality::Unspecified => "Unspecified",
            Temporality::Delta => "Delta",
            Temporality::Cumulative => "Cumulative",
        }
    }
}

/// Summary 的一个分位点。
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Quantile {
    /// 0..=1，比如 0.99。
    pub quantile: f64,
    pub value: f64,
}

/// 采样点：指标上挂的一次具体观测，带着当时的 trace / span id。
///
/// 这是指标和 trace 之间唯一的直连：面板上看到某个桶突然变高，用这里的 `trace_id`
/// 就能跳到 tracepipe 那张表里的具体请求。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Exemplar {
    /// UNIX 纳秒。
    pub timestamp: u64,
    pub value: f64,
    /// 32 位小写 hex，和 tracepipe / logpipe 落库的 `trace_id` 精确相等；没有则空串。
    pub trace_id: String,
    /// 16 位小写 hex。
    pub span_id: String,
    /// OTLP 叫 `filtered_attributes`：数据点本身没有、但这次观测有的那些属性。
    pub attributes: Attributes,
}

/// 一个数据点。类型不同，有意义的列也不同：
///
/// * `Gauge` / `Sum`：看 [`value`](Self::value)；`Sum` 另有 `temporality` / `is_monotonic`；
/// * `Histogram`：看 `count` / `sum` / `min` / `max` / `bucket_counts` / `explicit_bounds`；
/// * `ExponentialHistogram`：看 `count` / `sum` / `scale` / `zero_count` / `positive_*` / `negative_*`；
/// * `Summary`：看 `count` / `sum` / `quantiles`。
///
/// 用不上的列留默认值（数值 0、数组空）。OTLP 里 `sum` / `min` / `max` 是可选的，
/// 没上报同样是 0 —— 要区分「没有」和「就是 0」，看 `count` 和 `flags`。
#[derive(Clone, Debug, Default)]
pub struct MetricEvent {
    /// 数据点时间，UNIX 纳秒。
    pub timestamp: u64,
    /// 这一段累计的起点，UNIX 纳秒；没上报是 0。
    pub start_timestamp: u64,
    /// 指标名，比如 `http.server.request.duration`。同一个指标下的数据点共享一份。
    pub metric_name: Arc<str>,
    pub metric_type: MetricType,
    /// UCUM 单位，比如 `ms`、`By`、`1`。
    pub metric_unit: Arc<str>,
    pub metric_description: Arc<str>,
    /// resource 里的 `service.name`，没有则是 `unknown_service`。同一个 resource 下的
    /// 所有数据点共享一份。
    pub service_name: Arc<str>,
    pub scope_name: Arc<str>,
    pub scope_version: Arc<str>,
    /// 同一个 resource 下的所有数据点共享一份，逐条只加引用计数。
    pub resource_attributes: Arc<Attributes>,
    /// 数据点自己的属性，也就是这条时间线的标签（`http.route`、`status_code`……）。
    pub attributes: Attributes,
    /// Gauge / Sum 的值。整数上报的也转成 f64，一列装得下。
    pub value: f64,
    /// Sum / Histogram / ExponentialHistogram 有；Gauge / Summary 是 `Unspecified`。
    pub temporality: Temporality,
    /// Sum 专有：单调递增（counter）还是可增可减（up-down counter）。
    pub is_monotonic: bool,
    /// Histogram / ExponentialHistogram / Summary：样本个数。
    pub count: u64,
    /// 同上：样本总和。
    pub sum: f64,
    /// Histogram / ExponentialHistogram 可选上报的最小 / 最大值。
    pub min: f64,
    pub max: f64,
    /// Histogram：每个桶的计数，比 `explicit_bounds` 多一个（最后一个是上界之外的）。
    pub bucket_counts: Vec<u64>,
    /// Histogram：桶的上界，递增。
    pub explicit_bounds: Vec<f64>,
    /// ExponentialHistogram：桶宽的指数，base = 2^(2^-scale)。
    pub scale: i32,
    /// ExponentialHistogram：落在 0 附近（`|x| <= zero_threshold`）的样本数。
    pub zero_count: u64,
    pub zero_threshold: f64,
    /// ExponentialHistogram：正数侧桶的起始下标和计数。
    pub positive_offset: i32,
    pub positive_bucket_counts: Vec<u64>,
    /// 负数侧同上。
    pub negative_offset: i32,
    pub negative_bucket_counts: Vec<u64>,
    /// Summary：分位点。
    pub quantiles: Vec<Quantile>,
    /// 采样点，可用来跳到对应的 trace。
    pub exemplars: Vec<Exemplar>,
    /// OTLP 的 `DataPointFlags`；bit 0 = 这条时间线没数据（Prometheus 的 staleness）。
    pub flags: u32,
    /// 额外字段，落库前由调用方自行追加。
    pub fields: BTreeMap<String, Value>,
    /// 来源级的固定字段（配置里的静态 `fields`）。所有事件共享一份；序列化时和
    /// `fields` 一样平铺进 JSON，同名时 `fields` 里的优先。
    pub shared: Option<Arc<BTreeMap<String, Value>>>,
}

impl MetricEvent {
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Value>) -> Option<Value> {
        self.fields.insert(key.into(), value.into())
    }

    /// 先查 `fields`，再查 `shared`。
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields
            .get(key)
            .or_else(|| self.shared.as_ref().and_then(|shared| shared.get(key)))
    }

    /// 先查数据点属性，再查 resource 属性。
    pub fn attribute(&self, key: &str) -> Option<&Value> {
        self.attributes
            .get(key)
            .or_else(|| self.resource_attributes.get(key))
    }

    /// 估算编码成一行 `JSONCompactEachRow` 后的字节数，用于按体积攒批。
    ///
    /// 只是量级估计：两个时间戳、三十来个数字和空数组的占位大约 100 字节，其余按
    /// 字符串长度累加。带列名的 `JSONEachRow` 每行还要多五百多字节，这里不算它。
    pub fn estimated_size(&self) -> usize {
        fn attrs(map: &Attributes) -> usize {
            map.iter()
                .map(|(k, v)| k.len() + estimated_value_size(v) + 4)
                .sum::<usize>()
                + 2
        }
        let fixed = self.metric_name.len()
            + self.metric_unit.len()
            + self.metric_description.len()
            + self.service_name.len()
            + self.scope_name.len()
            + self.scope_version.len();
        // 数字在 JSON 里按十进制展开，一个给 20 字节够宽
        let arrays = 20
            * (self.bucket_counts.len()
                + self.explicit_bounds.len()
                + self.positive_bucket_counts.len()
                + self.negative_bucket_counts.len()
                + self.quantiles.len() * 2);
        let exemplars: usize = self
            .exemplars
            .iter()
            .map(|e| 100 + e.trace_id.len() + e.span_id.len() + attrs(&e.attributes))
            .sum();
        let entry = |(k, v): (&String, &Value)| k.len() + estimated_value_size(v) + 4;
        let extra: usize = self.fields.iter().map(entry).sum::<usize>()
            + self
                .shared
                .as_ref()
                .map_or(0, |shared| shared.iter().map(entry).sum());
        fixed
            + attrs(&self.resource_attributes)
            + attrs(&self.attributes)
            + arrays
            + exemplars
            + extra
            + 100
    }

    /// 编码成一行 JSON（ClickHouse `JSONEachRow`），时间戳按 UTC。
    pub fn to_json_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// 便于人读的一行，用于 console sink。
    pub fn to_text_line(&self) -> String {
        let value = if self.metric_type.is_scalar() {
            format!("{}", self.value)
        } else {
            format!("count={} sum={}", self.count, self.sum)
        };
        let labels = self
            .attributes
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "{} {} {} {} {value}{}{labels}",
            format_timestamp(self.timestamp, Tz::UTC).as_str(),
            self.service_name,
            self.metric_name,
            self.metric_type.as_str(),
            if labels.is_empty() { "" } else { " " },
        )
    }
}

fn estimated_value_size(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(_) => 5,
        Value::Number(_) => 8,
        Value::String(s) => s.len() + 2,
        Value::Array(a) => 2 + a.iter().map(estimated_value_size).sum::<usize>() + a.len(),
        Value::Object(o) => {
            2 + o
                .iter()
                .map(|(k, v)| k.len() + estimated_value_size(v) + 4)
                .sum::<usize>()
        }
    }
}

/// 固定列的名字，顺序就是 [`MetricEvent`] 写出各列的顺序，也是 ClickHouse sink 建表
/// 时列的顺序。`JSONCompactEachRow` 没有列名、纯靠位置对齐，两边都以这份为准。
pub const FIXED_COLUMNS: [&str; 35] = [
    "timestamp",
    "start_timestamp",
    "metric_name",
    "metric_type",
    "metric_unit",
    "metric_description",
    "service_name",
    "scope_name",
    "scope_version",
    "resource_attributes",
    "attributes",
    "value",
    "temporality",
    "is_monotonic",
    "count",
    "sum",
    "min",
    "max",
    "bucket_counts",
    "explicit_bounds",
    "scale",
    "zero_count",
    "zero_threshold",
    "positive_offset",
    "positive_bucket_counts",
    "negative_offset",
    "negative_bucket_counts",
    "quantiles.quantile",
    "quantiles.value",
    "exemplars.timestamp",
    "exemplars.value",
    "exemplars.trace_id",
    "exemplars.span_id",
    "exemplars.attributes",
    "flags",
];

/// 平铺成一层 JSON，时间戳按 UTC 带 `+00:00`。ClickHouse sink 配了时区用 [`WithZone`]。
impl Serialize for MetricEvent {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.serialize_in(serializer, Tz::UTC)
    }
}

/// 时间戳按指定时区换成墙上时间再序列化：`2026-09-07 11:04:08.914293456+08:00`。
///
/// 偏移一定带着：存进去的绝对时刻不依赖列有没有标时区，列上的时区只决定「查出来
/// 显示成几点」。
pub struct WithZone<'a> {
    pub event: &'a MetricEvent,
    pub tz: Tz,
}

impl Serialize for WithZone<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.event.serialize_in(serializer, self.tz)
    }
}

/// 一行 `JSONCompactEachRow`：只有值、没有列名的 JSON 数组，顺序是 [`FIXED_COLUMNS`]
/// 再接 `extra` 里点名的静态字段列（从 `fields` / `shared` 里取，没有的写 `null`）。
///
/// 比 [`WithZone`] 的 JSON 对象少掉每行五百多字节的列名：本地少序列化、少压缩，
/// ClickHouse 那边也不用逐行按 key 找列。
pub struct CompactRow<'a> {
    pub event: &'a MetricEvent,
    pub tz: Tz,
    /// 固定列之后还要写哪几列。ClickHouse sink 用的是它 `extra_columns` 的列名。
    pub extra: &'a [String],
}

impl Serialize for CompactRow<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let seq = serializer.serialize_seq(Some(FIXED_COLUMNS.len() + self.extra.len()))?;
        let mut columns = SeqColumns(seq);
        self.event.fixed_columns(&mut columns, self.tz)?;
        for name in self.extra {
            columns.0.serialize_element(&self.event.get(name))?;
        }
        columns.0.end()
    }
}

/// 一列一列地往外写；JSON 对象（带列名）和 JSON 数组（只有值）两种落法共用一套列表。
trait Columns {
    type Error;
    fn column<T: Serialize + ?Sized>(
        &mut self,
        name: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>;
}

struct MapColumns<M>(M);

impl<M: SerializeMap> Columns for MapColumns<M> {
    type Error = M::Error;
    fn column<T: Serialize + ?Sized>(
        &mut self,
        name: &'static str,
        value: &T,
    ) -> Result<(), M::Error> {
        self.0.serialize_entry(name, value)
    }
}

struct SeqColumns<S>(S);

impl<S: SerializeSeq> Columns for SeqColumns<S> {
    type Error = S::Error;
    fn column<T: Serialize + ?Sized>(
        &mut self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), S::Error> {
        self.0.serialize_element(value)
    }
}

/// 把一个迭代器当 JSON 数组序列化，省得为每个数组列各建一个 Vec。
struct Seq<I>(I);

impl<I> Serialize for Seq<I>
where
    I: Iterator + Clone,
    I::Item: Serialize,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.clone())
    }
}

/// 时间戳序列化成带偏移的墙上时间。
struct Ts(u64, Tz);

impl Serialize for Ts {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(format_timestamp(self.0, self.1).as_str())
    }
}

impl MetricEvent {
    /// 列的顺序和 [`crate::sink::ClickhouseSink`] 的建表语句一致。quantiles / exemplars
    /// 按 ClickHouse `Nested` 的平铺写法给：`exemplars.value` 等各是一个等长数组。
    fn serialize_in<S: Serializer>(&self, serializer: S, tz: Tz) -> Result<S::Ok, S::Error> {
        let map = serializer.serialize_map(None)?;
        let mut columns = MapColumns(map);
        self.fixed_columns(&mut columns, tz)?;
        let mut map = columns.0;
        if let Some(shared) = &self.shared {
            for (key, value) in shared.iter() {
                if !self.fields.contains_key(key) {
                    map.serialize_entry(key, value)?;
                }
            }
        }
        for (key, value) in &self.fields {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }

    /// 固定的 35 列，顺序和 [`FIXED_COLUMNS`] 逐一对应（有测试盯着）。
    fn fixed_columns<C: Columns>(&self, out: &mut C, tz: Tz) -> Result<(), C::Error> {
        out.column("timestamp", &Ts(self.timestamp, tz))?;
        out.column("start_timestamp", &Ts(self.start_timestamp, tz))?;
        out.column("metric_name", &*self.metric_name)?;
        out.column("metric_type", self.metric_type.as_str())?;
        out.column("metric_unit", &*self.metric_unit)?;
        out.column("metric_description", &*self.metric_description)?;
        out.column("service_name", &*self.service_name)?;
        out.column("scope_name", &*self.scope_name)?;
        out.column("scope_version", &*self.scope_version)?;
        out.column("resource_attributes", &*self.resource_attributes)?;
        out.column("attributes", &self.attributes)?;
        out.column("value", &self.value)?;
        out.column("temporality", self.temporality.as_str())?;
        // UInt8 列：写 0/1 而不是 true/false
        out.column("is_monotonic", &u8::from(self.is_monotonic))?;
        out.column("count", &self.count)?;
        out.column("sum", &self.sum)?;
        out.column("min", &self.min)?;
        out.column("max", &self.max)?;
        out.column("bucket_counts", &self.bucket_counts)?;
        out.column("explicit_bounds", &self.explicit_bounds)?;
        out.column("scale", &self.scale)?;
        out.column("zero_count", &self.zero_count)?;
        out.column("zero_threshold", &self.zero_threshold)?;
        out.column("positive_offset", &self.positive_offset)?;
        out.column("positive_bucket_counts", &self.positive_bucket_counts)?;
        out.column("negative_offset", &self.negative_offset)?;
        out.column("negative_bucket_counts", &self.negative_bucket_counts)?;
        out.column(
            "quantiles.quantile",
            &Seq(self.quantiles.iter().map(|q| q.quantile)),
        )?;
        out.column(
            "quantiles.value",
            &Seq(self.quantiles.iter().map(|q| q.value)),
        )?;
        out.column(
            "exemplars.timestamp",
            &Seq(self.exemplars.iter().map(|e| Ts(e.timestamp, tz))),
        )?;
        out.column(
            "exemplars.value",
            &Seq(self.exemplars.iter().map(|e| e.value)),
        )?;
        out.column(
            "exemplars.trace_id",
            &Seq(self.exemplars.iter().map(|e| &e.trace_id)),
        )?;
        out.column(
            "exemplars.span_id",
            &Seq(self.exemplars.iter().map(|e| &e.span_id)),
        )?;
        out.column(
            "exemplars.attributes",
            &Seq(self.exemplars.iter().map(|e| &e.attributes)),
        )?;
        out.column("flags", &self.flags)
    }
}

/// 格式化好的时间戳：`2026-09-07 11:04:08.914293456+08:00`，定长 35 字节。
pub struct Timestamp([u8; 35]);

impl Timestamp {
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("format_timestamp 只写 ASCII")
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// UNIX 纳秒 → `tz` 里的墙上时间，纳秒精度，末尾带该时刻的偏移。
///
/// 按位写进栈上的定长缓冲而不是 `format!`：chrono 的 `format(...)` 每次都要重新解析
/// 格式串再分配，一条记录光时间戳就有两三个，这里是序列化的大头。
pub fn format_timestamp(nanos: u64, tz: Tz) -> Timestamp {
    let secs = (nanos / 1_000_000_000) as i64;
    let sub = (nanos % 1_000_000_000) as u32;
    // u64 纳秒最多到 2554 年，一定在 chrono 的范围内
    let utc = DateTime::<Utc>::from_timestamp(secs, sub).unwrap_or(DateTime::UNIX_EPOCH);
    let local = utc.with_timezone(&tz);
    let offset = local.offset().fix().local_minus_utc();

    fn put(slot: &mut [u8], mut value: u32) {
        for byte in slot.iter_mut().rev() {
            *byte = b'0' + (value % 10) as u8;
            value /= 10;
        }
    }

    let mut buf = *b"0000-00-00 00:00:00.000000000+00:00";
    put(&mut buf[0..4], local.year().clamp(0, 9999) as u32);
    put(&mut buf[5..7], local.month());
    put(&mut buf[8..10], local.day());
    put(&mut buf[11..13], local.hour());
    put(&mut buf[14..16], local.minute());
    put(&mut buf[17..19], local.second());
    put(&mut buf[20..29], sub);
    if offset < 0 {
        buf[29] = b'-';
    }
    let minutes = offset.unsigned_abs() / 60;
    put(&mut buf[30..32], minutes / 60);
    put(&mut buf[33..35], minutes % 60);
    Timestamp(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// 2026-09-07 03:04:08.914293456 UTC
    fn nanos() -> u64 {
        let secs = Utc
            .with_ymd_and_hms(2026, 9, 7, 3, 4, 8)
            .unwrap()
            .timestamp() as u64;
        secs * 1_000_000_000 + 914_293_456
    }

    fn histogram() -> MetricEvent {
        MetricEvent {
            timestamp: nanos(),
            start_timestamp: nanos() - 60_000_000_000,
            metric_name: Arc::from("http.server.request.duration"),
            metric_type: MetricType::Histogram,
            metric_unit: Arc::from("ms"),
            service_name: Arc::from("order-service"),
            temporality: Temporality::Cumulative,
            count: 7,
            sum: 123.5,
            min: 1.0,
            max: 90.0,
            bucket_counts: vec![3, 3, 1],
            explicit_bounds: vec![5.0, 50.0],
            attributes: [("http.route".to_owned(), Value::from("/orders/{id}"))]
                .into_iter()
                .collect(),
            exemplars: vec![Exemplar {
                timestamp: nanos() + 1_000,
                value: 90.0,
                trace_id: "e89a476882236ce0f1186d1522c8f59f".into(),
                span_id: "e8b0e73e2132f21c".into(),
                attributes: Attributes::new(),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn formats_wall_clock_with_offset() {
        assert_eq!(
            format_timestamp(nanos(), Tz::UTC).as_str(),
            "2026-09-07 03:04:08.914293456+00:00"
        );
        assert_eq!(
            format_timestamp(nanos(), chrono_tz::Asia::Shanghai).as_str(),
            "2026-09-07 11:04:08.914293456+08:00"
        );
        assert_eq!(
            format_timestamp(nanos(), chrono_tz::America::New_York).as_str(),
            "2026-09-06 23:04:08.914293456-04:00"
        );
        // 和 chrono 自己的格式化一致
        let expect = DateTime::<Utc>::from_timestamp(nanos() as i64 / 1_000_000_000, 914_293_456)
            .unwrap()
            .with_timezone(&chrono_tz::Asia::Shanghai)
            .format("%Y-%m-%d %H:%M:%S%.9f%:z")
            .to_string();
        assert_eq!(
            format_timestamp(nanos(), chrono_tz::Asia::Shanghai).as_str(),
            expect
        );
        assert_eq!(
            format_timestamp(0, Tz::UTC).as_str(),
            "1970-01-01 00:00:00.000000000+00:00"
        );
    }

    #[test]
    fn serializes_flat_with_nested_arrays() {
        let mut event = histogram();
        event.shared = Some(Arc::new(
            [("cluster".to_owned(), Value::from("bj-prod"))]
                .into_iter()
                .collect(),
        ));
        event.insert("env", "prod");

        let json: Value = serde_json::from_str(&event.to_json_line().unwrap()).unwrap();
        assert_eq!(json["timestamp"], "2026-09-07 03:04:08.914293456+00:00");
        assert_eq!(json["metric_name"], "http.server.request.duration");
        assert_eq!(json["metric_type"], "Histogram");
        assert_eq!(json["temporality"], "Cumulative");
        assert_eq!(json["count"], 7);
        assert_eq!(json["bucket_counts"], serde_json::json!([3, 3, 1]));
        assert_eq!(json["explicit_bounds"], serde_json::json!([5.0, 50.0]));
        assert_eq!(json["attributes"]["http.route"], "/orders/{id}");
        assert_eq!(
            json["exemplars.trace_id"],
            serde_json::json!(["e89a476882236ce0f1186d1522c8f59f"])
        );
        assert_eq!(
            json["exemplars.timestamp"][0],
            "2026-09-07 03:04:08.914294456+00:00"
        );
        // 用不上的列也占位，一行的列是齐的
        assert_eq!(json["quantiles.value"], serde_json::json!([]));
        assert_eq!(json["value"], 0.0);
        assert_eq!(json["is_monotonic"], 0);
        assert_eq!(json["cluster"], "bj-prod");
        assert_eq!(json["env"], "prod");
        assert_eq!(event.get("cluster"), Some(&Value::from("bj-prod")));
        assert_eq!(
            event.attribute("http.route"),
            Some(&Value::from("/orders/{id}"))
        );

        // 配了时区：墙上时间换算、偏移跟着变，其余不动
        let json: Value = serde_json::from_str(
            &serde_json::to_string(&WithZone {
                event: &event,
                tz: chrono_tz::Asia::Shanghai,
            })
            .unwrap(),
        )
        .unwrap();
        assert_eq!(json["timestamp"], "2026-09-07 11:04:08.914293456+08:00");
        assert_eq!(
            json["exemplars.timestamp"][0],
            "2026-09-07 11:04:08.914294456+08:00"
        );
        assert!(event.estimated_size() > MetricEvent::default().estimated_size());
    }

    /// `JSONCompactEachRow` 纯靠位置对齐：写出的列顺序必须和 `FIXED_COLUMNS` 一致。
    #[test]
    fn fixed_columns_match_the_declared_order() {
        struct Names(Vec<&'static str>);
        impl Columns for Names {
            type Error = std::fmt::Error;
            fn column<T: Serialize + ?Sized>(
                &mut self,
                name: &'static str,
                _: &T,
            ) -> Result<(), Self::Error> {
                self.0.push(name);
                Ok(())
            }
        }
        let mut names = Names(Vec::new());
        histogram().fixed_columns(&mut names, Tz::UTC).unwrap();
        assert_eq!(names.0, FIXED_COLUMNS);
    }

    /// 紧凑行的每个位置，和带列名的那种写法里同名的值一模一样；extra 列从
    /// `fields` / `shared` 里取，没有的是 null。
    #[test]
    fn compact_row_lines_up_with_the_map_form() {
        let mut event = histogram();
        event.shared = Some(Arc::new(
            [("cluster".to_owned(), Value::from("bj-prod"))]
                .into_iter()
                .collect(),
        ));
        event.insert("env", "prod");
        let tz = chrono_tz::Asia::Shanghai;

        let map: Value = serde_json::to_value(WithZone { event: &event, tz }).unwrap();
        let extra = ["cluster".to_owned(), "env".to_owned(), "absent".to_owned()];
        let row: Value = serde_json::to_value(CompactRow {
            event: &event,
            tz,
            extra: &extra,
        })
        .unwrap();
        let row = row.as_array().unwrap();

        assert_eq!(row.len(), FIXED_COLUMNS.len() + extra.len());
        for (i, name) in FIXED_COLUMNS.iter().enumerate() {
            assert_eq!(&row[i], &map[*name], "第 {i} 列 {name} 对不上");
        }
        assert_eq!(row[FIXED_COLUMNS.len()], "bj-prod");
        assert_eq!(row[FIXED_COLUMNS.len() + 1], "prod");
        assert_eq!(row[FIXED_COLUMNS.len() + 2], Value::Null);
        assert_eq!(row[0], "2026-09-07 11:04:08.914293456+08:00");
    }

    #[test]
    fn text_line_shows_value_or_summary() {
        let gauge = MetricEvent {
            timestamp: nanos(),
            metric_name: Arc::from("system.memory.usage"),
            metric_type: MetricType::Gauge,
            service_name: Arc::from("node"),
            value: 42.5,
            attributes: [("state".to_owned(), Value::from("used"))]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        assert_eq!(
            gauge.to_text_line(),
            "2026-09-07 03:04:08.914293456+00:00 node system.memory.usage Gauge 42.5 state=\"used\""
        );
        assert!(histogram().to_text_line().contains("count=7 sum=123.5"));
    }
}
