//! ClickHouse 入库：HTTP 接口 + `JSONCompactEachRow` 批量插入（可退回 `JSONEachRow`）。
//!
//! 建表语句见 [`ClickhouseSink::create_table_ddl`]，列与 [`MetricEvent`] 一一对应。
//! 五种指标类型合在一张表里（OTel collector 的 clickhouse exporter 是分五张表的），
//! 列名沿用它的 snake_case 写法，好和 logpipe / tracepipe 的表一起查。

use std::io::Write;
use std::time::Duration;

use async_trait::async_trait;
use chrono_tz::Tz;

use crate::error::{Error, Result};
use crate::event::{CompactRow, MetricEvent, WithZone, FIXED_COLUMNS};
use crate::sink::Sink;

/// INSERT 的请求体格式。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum InsertFormat {
    /// 一行一个 JSON 数组，只有值。INSERT 语句里带列名清单，按位置对上。默认。
    ///
    /// 比 [`JsonEachRow`](Self::JsonEachRow) 每行少五百多字节的列名（35 个列名约占
    /// 一行未压缩体积的四成），本地少序列化、少压缩，服务端也不用逐行按 key 找列。
    #[default]
    JsonCompactEachRow,
    /// 一行一个 JSON 对象，带列名。表里没有的字段靠 `input_format_skip_unknown_fields`
    /// 跳过。留着做退路：中间有代理改写 SQL、或者想肉眼看请求体的时候用。
    JsonEachRow,
}

impl InsertFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            InsertFormat::JsonCompactEachRow => "JSONCompactEachRow",
            InsertFormat::JsonEachRow => "JSONEachRow",
        }
    }
}

/// 序列化时每攒多少行就往压缩器里灌一次并让出一次调度。
///
/// 序列化和 gzip 都是同步 CPU 活，直接在 tokio worker 上一口气做完 20k 行要上百毫秒，
/// 同一个 worker 上排着的 gRPC 解码、应答全得等着。切成小块，每块几百微秒，中间
/// `yield_now` 让别的任务插进来。顺带也不用再攥着一份几十 MB 的未压缩 body。
const ROWS_PER_CHUNK: usize = 256;

/// `start_timestamp` 之后、`exemplars.*` 之前的固定列。两个时间戳列的类型跟着
/// `timezone` 走，不在这里。
const COLUMNS_HEAD: [(&str, &str); 25] = [
    ("metric_name", "LowCardinality(String)"),
    ("metric_type", "LowCardinality(String)"),
    ("metric_unit", "LowCardinality(String)"),
    ("metric_description", "String"),
    ("service_name", "LowCardinality(String)"),
    ("scope_name", "LowCardinality(String)"),
    ("scope_version", "LowCardinality(String)"),
    ("resource_attributes", "JSON"),
    // 这条时间线的标签。查询几乎都要按它过滤，JSON 列的子列是独立存储的，
    // `attributes.http.route` 只读那一个子列。
    ("attributes", "JSON"),
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
];

/// Summary 的分位点，`Nested` 的平铺写法：两个等长数组。
const COLUMNS_QUANTILES: [(&str, &str); 2] = [
    ("quantiles.quantile", "Array(Float64)"),
    ("quantiles.value", "Array(Float64)"),
];

/// `exemplars.timestamp` 之后的列，同样是平铺的 `Nested`。
const COLUMNS_TAIL: [(&str, &str); 5] = [
    ("exemplars.value", "Array(Float64)"),
    ("exemplars.trace_id", "Array(String)"),
    ("exemplars.span_id", "Array(String)"),
    ("exemplars.attributes", "Array(JSON)"),
    ("flags", "UInt32"),
];

/// 属性列的类型。`JSON` 列里每个 key 是一个独立的子列，查 `attributes.http.route`
/// 只读这一个子列，不用把整行属性解出来；值保留 OTLP 里的类型。需要 ClickHouse 25.3+
/// （JSON 类型在 25.3 转正）。healthcheck 也用它认老表。
pub const ATTRIBUTES_TYPE: &str = "JSON";

/// 吃类型提示和 SKIP 的属性列。exemplar 的属性不在内：那是稀疏的旁路数据，
/// 为它多建几个子列不划算。
const HINTED_COLUMNS: [&str; 2] = ["resource_attributes", "attributes"];

/// 跳数索引。
///
/// * `metric_name` 在排序键里排第二，只按指标名查（不给 service）时用不上，补一个
///   bloom filter；
/// * exemplar 的 trace id 是指标跳 trace 的入口，`has(exemplars.trace_id, '...')`
///   不带时间范围时会全表扫，同样交给 bloom filter。
///
/// 属性上不预建索引：`JSON` 列的跳数索引要建在带类型的子列上（比如
/// `attributes.http.route.:String`），哪些 key 值得建索引由查询决定，在 DDL 输出之外
/// 自己 `ADD INDEX`，README 有例子。
const INDEXES: [(&str, &str); 2] = [
    (
        "idx_metric_name",
        "`metric_name` TYPE bloom_filter GRANULARITY 4",
    ),
    (
        "idx_exemplar_trace_id",
        "`exemplars.trace_id` TYPE bloom_filter GRANULARITY 4",
    ),
];

pub struct ClickhouseSink {
    client: reqwest::Client,
    endpoint: String,
    database: String,
    table: String,
    cluster: Option<String>,
    timezone: Option<Tz>,
    /// 固定列之外还要有的列（配置里的静态字段），建表和启动校验都用。
    extra_columns: Vec<(String, String)>,
    /// 属性列里给指定路径的类型提示，见 [`Self::attribute_types`]。
    attribute_types: Vec<(String, String)>,
    /// 不入库的属性路径 / 正则，见 [`Self::attribute_skip`]。
    attribute_skip: Vec<String>,
    attribute_skip_regexp: Vec<String>,
    user: Option<String>,
    password: Option<String>,
    timeout: Duration,
    async_insert: bool,
    compress: bool,
    insert_format: InsertFormat,
}

impl ClickhouseSink {
    /// `endpoint` 形如 `http://127.0.0.1:8123`。
    pub fn new(
        endpoint: impl Into<String>,
        database: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        // 空闲连接留得比 ClickHouse 的 keep_alive_timeout（默认 10s 上下，老版本 3s）短：
        // 不然低流量时段的第一条 INSERT 会撞上服务端已经关掉的连接，POST 不会被 hyper
        // 自动重试，结果是一次假的写入失败加 500ms 退避。宁可多握一次手。
        let client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(2))
            .build()
            .expect("构造 reqwest 客户端");
        Self {
            client,
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            database: database.into(),
            table: table.into(),
            cluster: None,
            timezone: None,
            extra_columns: Vec::new(),
            attribute_types: Vec::new(),
            attribute_skip: Vec::new(),
            attribute_skip_regexp: Vec::new(),
            user: None,
            password: None,
            timeout: Duration::from_secs(30),
            async_insert: false,
            compress: true,
            insert_format: InsertFormat::default(),
        }
    }

    /// INSERT 请求体用哪种格式，默认 [`InsertFormat::JsonCompactEachRow`]。
    pub fn insert_format(mut self, format: InsertFormat) -> Self {
        self.insert_format = format;
        self
    }

    /// ClickHouse 集群名（`system.clusters` 里的那个，不是 k8s 集群）。
    ///
    /// 配了之后建表语句变成两张表：`<table>_local` 是 `ReplicatedMergeTree`，带
    /// `ON CLUSTER` 一次性下发到所有节点；`<table>` 是它上面的 `Distributed`，
    /// 也就是 sink 实际写入的那张。写入路径本身不受影响 —— 还是往 `table` 里 INSERT。
    pub fn cluster(mut self, cluster: impl Into<String>) -> Self {
        self.cluster = Some(cluster.into());
        self
    }

    /// 时间戳列的显示时区，比如 `Asia/Shanghai`。
    ///
    /// 数据点的时间是绝对时刻（UNIX 纳秒），INSERT 时总是带着偏移写出去
    /// （`2026-09-07 11:04:08.914293456+08:00`），存进去的时刻不依赖列的时区。这个
    /// 选项只影响 `--ddl`：时间戳列建成 `DateTime64(9, 'Asia/Shanghai')`，查出来显示的
    /// 是北京时间而不是服务端时区。要和 logpipe / tracepipe 的表按同一个墙上时间
    /// 对照着看就配上。
    pub fn timezone(mut self, timezone: Tz) -> Self {
        self.timezone = Some(timezone);
        self
    }

    /// 固定列之外的列：配置里的静态字段。`--ddl` 建出来，healthcheck 时校验表里确实有。
    pub fn extra_columns(mut self, columns: Vec<(String, String)>) -> Self {
        self.extra_columns = columns;
        self
    }

    /// 属性列里几条已知路径的类型提示，建表时写成 `JSON(http.route String, ...)`。
    ///
    /// 带提示的路径会物化成**类型确定**的独立子列，好处有两个：查询不用写
    /// `attributes.http.response.status_code.:Int64` 这种带类型转换的路径；不同服务
    /// 对同一个 key 发不同类型时（一个发整数一个发 `"200"`）也不会再 `NO_COMMON_TYPE`
    /// —— 对不上类型的值按 `null` 处理，而不是把这一列变成 Dynamic。
    ///
    /// 指标的标签 key 多半来自语义约定，类型是定死的，值得提示。只作用在
    /// `resource_attributes` 和 `attributes` 两列上（exemplar 的属性稀疏且量小，
    /// 不跟着建子列）。
    ///
    /// **只在建表时生效**：`ADD COLUMN IF NOT EXISTS` 见列已存在会跳过，给老表加提示
    /// 得自己 `MODIFY COLUMN`（会重写整列）或者重建表。healthcheck 发现表里没有配置
    /// 的提示会 warn 一句。
    pub fn attribute_types(mut self, types: Vec<(String, String)>) -> Self {
        self.attribute_types = types;
        self
    }

    /// 不入库的属性路径，建表时写成 `JSON(SKIP debug.payload)`。
    ///
    /// 用来把明知不想要的 key 挡在存储之外（临时调试标签、误埋的高基数 key）——
    /// 它们连子列都不会建。同样只在建表时生效。
    pub fn attribute_skip(mut self, paths: Vec<String>) -> Self {
        self.attribute_skip = paths;
        self
    }

    /// 同上，按正则跳过：`JSON(SKIP REGEXP '^debug\\..*')`。
    pub fn attribute_skip_regexp(mut self, patterns: Vec<String>) -> Self {
        self.attribute_skip_regexp = patterns;
        self
    }

    pub fn auth(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self.password = Some(password.into());
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// 打开后由 ClickHouse 服务端再攒一层批，适合多实例小批量写入的场景。
    pub fn async_insert(mut self, enabled: bool) -> Self {
        self.async_insert = enabled;
        self
    }

    /// 是否 gzip 压缩 INSERT 的请求体，默认开。
    ///
    /// 指标的 JSON 里同一个指标名、同一批属性 key 反复出现，压得很动。关掉它一般只有
    /// 一个理由：中间的代理/网关不能正确转发压缩过的 body。
    pub fn compress(mut self, enabled: bool) -> Self {
        self.compress = enabled;
        self
    }

    fn timestamp_type(&self) -> String {
        match &self.timezone {
            Some(tz) => format!("DateTime64(9, '{}')", tz.name()),
            None => "DateTime64(9)".to_owned(),
        }
    }

    /// `resource_attributes` / `attributes` 两列的类型：光秃秃的 `JSON`，或者带上
    /// 路径提示和 SKIP。
    ///
    /// 参数的顺序按 ClickHouse 文档来：先类型提示，再 `SKIP`，最后 `SKIP REGEXP`。
    fn attributes_type(&self) -> String {
        let mut params: Vec<String> = self
            .attribute_types
            .iter()
            .map(|(path, ty)| format!("{path} {ty}"))
            .collect();
        params.extend(
            self.attribute_skip
                .iter()
                .map(|path| format!("SKIP {path}")),
        );
        params.extend(
            self.attribute_skip_regexp
                .iter()
                .map(|pattern| format!("SKIP REGEXP '{}'", escape_literal(pattern))),
        );

        if params.is_empty() {
            ATTRIBUTES_TYPE.to_owned()
        } else {
            format!("{ATTRIBUTES_TYPE}({})", params.join(", "))
        }
    }

    /// 全部固定列，按建表顺序。
    fn base_columns(&self) -> Vec<(String, String)> {
        let ts = self.timestamp_type();
        let attributes = self.attributes_type();
        let mut columns = vec![
            ("timestamp".to_owned(), ts.clone()),
            ("start_timestamp".to_owned(), ts.clone()),
        ];
        let owned = |(name, ty): &(&str, &str)| ((*name).to_owned(), (*ty).to_owned());
        // 两个属性列的类型跟着 attribute_types / attribute_skip 走
        columns.extend(
            COLUMNS_HEAD
                .iter()
                .map(|(name, ty)| match HINTED_COLUMNS.contains(name) {
                    true => ((*name).to_owned(), attributes.clone()),
                    false => owned(&(*name, *ty)),
                }),
        );
        columns.extend(COLUMNS_QUANTILES.iter().map(owned));
        columns.push(("exemplars.timestamp".to_owned(), format!("Array({ts})")));
        columns.extend(COLUMNS_TAIL.iter().map(owned));
        columns
    }

    /// 建表 + 补表的语句，直接拿去执行即可，重复执行也没事。
    ///
    /// 第一段 `CREATE TABLE IF NOT EXISTS` 管新表；后面的 `ALTER TABLE` 全是
    /// `ADD COLUMN IF NOT EXISTS` / `ADD INDEX IF NOT EXISTS` / `MODIFY COLUMN`
    /// 这类幂等操作，管老表：配置里新加了 `fields`、后来配了 `timezone`，重跑一次就把
    /// 差异补齐，不用人手对着表结构写 ALTER。集群模式下 `_local` 表和 `Distributed`
    /// 表各补一遍，`Distributed` 不会自动跟着本地表变列，而且它不支持跳数索引。
    ///
    /// 不碰的：排序键、分区键改不了；TTL 能改但 `MODIFY TTL` 会触发重算；已有列的类型
    /// 变了（静态字段从整数改成字符串）`IF NOT EXISTS` 会跳过 —— 这几种本来就该人看
    /// 一眼再动。
    pub fn create_table_ddl(&self) -> String {
        self.create_table_ddl_with(&self.extra_columns)
    }

    /// 同上，额外追加几列而不用 [`Self::extra_columns`]。
    pub fn create_table_ddl_with(&self, extra: &[(String, String)]) -> String {
        let timestamp_type = self.timestamp_type();
        let mut columns = self.base_columns();
        for (name, ty) in extra {
            if !columns.iter().any(|(existing, _)| existing == name) {
                columns.push((name.clone(), ty.clone()));
            }
        }

        let width = columns
            .iter()
            .map(|(name, _)| name.len())
            .max()
            .unwrap_or(0);
        let pad = |name: &str| " ".repeat(width - name.len());
        let mut body: Vec<String> = columns
            .iter()
            .map(|(name, ty)| format!("    `{name}`{} {ty}", pad(name)))
            .collect();
        body.extend(
            INDEXES
                .iter()
                .map(|(name, expr)| format!("    INDEX `{name}` {expr}")),
        );
        let body = body.join(",\n");

        // 排序键按查询的写法来：几乎所有面板都是「某个服务的某个指标，最近一段时间」，
        // 再按标签细分 —— 所以 service_name / metric_name 在前，同一条时间线的点
        // 挨着放，Float64 那几列的压缩率也跟着上去。
        let layout = "PARTITION BY toDate(`timestamp`)\n\
             ORDER BY (`service_name`, `metric_name`, toDateTime(`timestamp`))\n\
             TTL toDateTime(`timestamp`) + INTERVAL 30 DAY";
        let db = &self.database;
        let table = &self.table;
        // 老表补时区补不了 `timestamp`：它在排序键和分区键里，ClickHouse 不允许 ALTER
        // 键列（ALTER_OF_COLUMN_IS_FORBIDDEN），改时区也不行。存的时刻不受影响
        // （INSERT 一律带偏移），只是查出来按服务端时区显示，要换显示时区只能重建表。
        let modify_timestamps: Vec<String> = match self.timezone {
            Some(_) => vec![
                format!("MODIFY COLUMN `start_timestamp` {timestamp_type}"),
                format!("MODIFY COLUMN `exemplars.timestamp` Array({timestamp_type})"),
            ],
            None => Vec::new(),
        };

        let alter = |target: &str, on_cluster: &str, with_index: bool| {
            // timestamp 排第一、在排序键里，一定存在，不 ADD
            let mut actions: Vec<String> = columns
                .iter()
                .filter(|(name, _)| name != "timestamp")
                .map(|(name, ty)| format!("ADD COLUMN IF NOT EXISTS `{name}` {ty}"))
                .collect();
            if with_index {
                actions.extend(
                    INDEXES
                        .iter()
                        .map(|(name, expr)| format!("ADD INDEX IF NOT EXISTS `{name}` {expr}")),
                );
            }
            actions.extend(modify_timestamps.iter().cloned());
            format!(
                "ALTER TABLE `{db}`.`{target}`{on_cluster}\n    {}",
                actions.join(",\n    ")
            )
        };

        let Some(cluster) = &self.cluster else {
            return format!(
                "CREATE TABLE IF NOT EXISTS `{db}`.`{table}`\n\
                 (\n{body}\n)\n\
                 ENGINE = MergeTree\n{layout};\n\n{}",
                alter(table, "", true)
            );
        };

        // 集群：本地表存数据，Distributed 表负责分发，全部 ON CLUSTER 一次下发。
        // `{shard}` / `{replica}` 是 ClickHouse 自己的宏，由各节点的 macros 配置展开。
        // 补列先补本地表再补 Distributed 表：反过来的话中间那一瞬间往 Distributed 表
        // 插新列会因为本地表没有而失败。分片键用 service_name + metric_name 的 hash：
        // 同一条时间线始终落同一个分片，按时间线聚合不用跨分片。
        let local = self.local_table();
        let on_cluster = format!(" ON CLUSTER `{cluster}`");
        format!(
            "CREATE TABLE IF NOT EXISTS `{db}`.`{local}`{on_cluster}\n\
             (\n{body}\n)\n\
             ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{db}/{local}', '{{replica}}')\n\
             {layout};\n\n\
             CREATE TABLE IF NOT EXISTS `{db}`.`{table}`{on_cluster}\n\
             AS `{db}`.`{local}`\n\
             ENGINE = Distributed(`{cluster}`, `{db}`, `{local}`, cityHash64(`service_name`, `metric_name`));\n\n\
             {};\n\n{}",
            alter(&local, &on_cluster, true),
            alter(table, &on_cluster, false)
        )
    }

    /// 属性列的类型里应当出现的片段（路径提示的路径名、SKIP 的路径 / 正则），
    /// healthcheck 拿它对一遍表上的列类型。
    ///
    /// 只比对片段而不是整个类型串：ClickHouse 存回来的类型是它自己规整过的写法，
    /// 逐字符比对会因为空格、引号这些差异误报。
    fn attribute_hints(&self) -> impl Iterator<Item = String> + '_ {
        self.attribute_types
            .iter()
            .map(|(path, _)| path.clone())
            .chain(self.attribute_skip.iter().cloned())
            .chain(self.attribute_skip_regexp.iter().cloned())
    }

    /// 表里必须有的列名：固定列 + 额外列。
    fn expected_columns(&self) -> Vec<String> {
        self.base_columns()
            .into_iter()
            .map(|(name, _)| name)
            .chain(self.extra_columns.iter().map(|(name, _)| name.clone()))
            .collect()
    }

    /// 集群模式下真正存数据的本地表名：`<table>_local`。
    pub fn local_table(&self) -> String {
        format!("{}_local", self.table)
    }

    /// INSERT 时写哪几列、按什么顺序：固定列（[`FIXED_COLUMNS`]）再接静态字段列。
    /// `JSONCompactEachRow` 的每一行就按这个顺序给值。
    pub fn insert_columns(&self) -> Vec<String> {
        FIXED_COLUMNS
            .iter()
            .map(|name| (*name).to_owned())
            .chain(self.extra_columns.iter().map(|(name, _)| name.clone()))
            .collect()
    }

    /// INSERT 语句。紧凑格式带列名清单（Nested 的子列 `exemplars.value` 反引号包着
    /// 就能点名），对象格式让服务端自己按 key 对。
    fn insert_sql(&self) -> String {
        match self.insert_format {
            InsertFormat::JsonCompactEachRow => {
                let columns: Vec<String> = self
                    .insert_columns()
                    .into_iter()
                    .map(|name| format!("`{name}`"))
                    .collect();
                format!(
                    "INSERT INTO `{}`.`{}` ({}) FORMAT JSONCompactEachRow",
                    self.database,
                    self.table,
                    columns.join(", ")
                )
            }
            InsertFormat::JsonEachRow => format!(
                "INSERT INTO `{}`.`{}` FORMAT JSONEachRow",
                self.database, self.table
            ),
        }
    }

    /// 执行任意 SQL（建表、查询都可以）。
    pub async fn execute(&self, sql: &str) -> Result<String> {
        self.request(sql, Vec::new(), false).await
    }

    /// `compressed` 说明 `body` 已经是 gzip 过的。空 body（`SELECT 1`、`EXISTS TABLE`
    /// 这些健康检查）一律不压：gzip 一个空串反而会多出十几个字节的头，而这里正是
    /// 411 那个坑所在，保持原样最稳。
    async fn request(&self, sql: &str, body: Vec<u8>, compressed: bool) -> Result<String> {
        let mut settings: Vec<(&str, &str)> = vec![
            ("query", sql),
            // 时间戳按 `2026-09-07 03:04:08.914293456+00:00` 发送，要开宽松解析
            ("date_time_input_format", "best_effort"),
            // 指标的值可能是 NaN / Inf（Prometheus 的 staleness marker、除零得到的
            // 速率……），而 JSON 里没有这几个字面量的写法，serde 会写成 null。没有这个
            // 设置的话整批插入会因为「Float64 列收到 null」失败 —— 一条脏数据带走一批。
            // 代价是这些点落库成 0，要区分的话看 flags 那一列。
            ("input_format_null_as_default", "1"),
        ];
        if self.insert_format == InsertFormat::JsonEachRow {
            // 事件里的自定义字段可能没有对应列，跳过而不是整批失败。紧凑格式没有
            // key，写哪几列是 INSERT 语句说了算，用不上这条。
            settings.push(("input_format_skip_unknown_fields", "1"));
        }
        if self.async_insert {
            settings.push(("async_insert", "1"));
            settings.push(("wait_for_async_insert", "1"));
        }

        // Content-Length 必须自己写。body 为空时 hyper 认为流已经结束，既不发
        // Content-Length 也不用 chunked，而 ClickHouse 见到这样的 POST 直接回
        // 411 Length Required。
        let mut request = self
            .client
            .post(&self.endpoint)
            .query(&settings)
            .timeout(self.timeout)
            .header(reqwest::header::CONTENT_LENGTH, body.len())
            .body(body);

        if compressed {
            request = request.header(reqwest::header::CONTENT_ENCODING, "gzip");
        }

        if let (Some(user), Some(password)) = (&self.user, &self.password) {
            request = request
                .header("X-ClickHouse-User", user)
                .header("X-ClickHouse-Key", password);
        }

        let response = request.send().await.map_err(Error::sink)?;
        let status = response.status();
        let text = response.text().await.map_err(Error::sink)?;

        if !status.is_success() {
            return Err(Error::Sink(
                format!("ClickHouse 返回 {status}: {}", text.trim()).into(),
            ));
        }
        Ok(text)
    }
}

/// 三个属性列，healthcheck 校验类型用。
const ATTRIBUTE_COLUMNS: [&str; 3] = ["resource_attributes", "attributes", "exemplars.attributes"];

/// `system.columns` 里的类型是不是期望的 JSON 类型。带参数的写法
/// （`JSON(max_dynamic_paths=2048)`、`Array(JSON(...))`）也算：参数是人按需要调的。
fn is_json_type(actual: &str, expected: &str) -> bool {
    let actual = actual.replace(' ', "");
    let expected = expected.replace(' ', "");
    if actual == expected {
        return true;
    }
    match expected.strip_prefix("Array(") {
        Some(inner) => actual
            .strip_prefix("Array(")
            .and_then(|a| a.strip_suffix(')'))
            .is_some_and(|a| is_json_type(a, inner.trim_end_matches(')'))),
        None => actual.starts_with("JSON(") && actual.ends_with(')'),
    }
}

/// SQL 字符串字面量转义：库名表名来自配置，反引号标识符走的是另一套规则，这里只管
/// `WHERE database = '...'` 里的单引号串。
fn escape_literal(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('\'', "\\'")
}

/// 攒 INSERT 请求体：要压就边序列化边喂给 gzip，不压就原样堆着。
///
/// ClickHouse 见到 `Content-Encoding: gzip` 会自己解开，服务端不用开任何设置 ——
/// `enable_http_compression` 管的是响应方向，跟这里无关。压缩级别取最快的那一档：
/// 多压那百分之十几的体积要多花几倍 CPU，不划算。
enum BodyBuffer {
    Gzip(flate2::write::GzEncoder<Vec<u8>>),
    Plain(Vec<u8>),
}

impl BodyBuffer {
    fn new(compress: bool, rows: usize) -> Self {
        if compress {
            // 指标行压得很动（三十倍上下），每行留 64 字节的输出空间够了
            BodyBuffer::Gzip(flate2::write::GzEncoder::new(
                Vec::with_capacity(rows * 64),
                flate2::Compression::fast(),
            ))
        } else {
            BodyBuffer::Plain(Vec::with_capacity(rows * 1024))
        }
    }

    fn push(&mut self, chunk: &[u8]) -> Result<()> {
        match self {
            BodyBuffer::Gzip(encoder) => encoder
                .write_all(chunk)
                .map_err(|err| Error::io("压缩 ClickHouse 请求体失败".to_owned(), err)),
            BodyBuffer::Plain(buf) => {
                buf.extend_from_slice(chunk);
                Ok(())
            }
        }
    }

    /// 交出请求体和「压过了没」。
    fn finish(self) -> Result<(Vec<u8>, bool)> {
        match self {
            BodyBuffer::Gzip(encoder) => encoder
                .finish()
                .map(|body| (body, true))
                .map_err(|err| Error::io("压缩 ClickHouse 请求体失败".to_owned(), err)),
            BodyBuffer::Plain(buf) => Ok((buf, false)),
        }
    }
}

#[async_trait]
impl Sink for ClickhouseSink {
    async fn write(&mut self, events: &[MetricEvent]) -> Result<()> {
        // 时间戳一律带偏移；没配时区就按 UTC 换算
        let tz = self.timezone.unwrap_or(Tz::UTC);
        let extra: Vec<String> = self
            .extra_columns
            .iter()
            .map(|(name, _)| name.clone())
            .collect();

        let mut body = BodyBuffer::new(self.compress, events.len());
        let mut chunk: Vec<u8> = Vec::with_capacity(ROWS_PER_CHUNK * 1024);
        for rows in events.chunks(ROWS_PER_CHUNK) {
            for event in rows {
                match self.insert_format {
                    InsertFormat::JsonCompactEachRow => serde_json::to_writer(
                        &mut chunk,
                        &CompactRow {
                            event,
                            tz,
                            extra: &extra,
                        },
                    )?,
                    InsertFormat::JsonEachRow => {
                        serde_json::to_writer(&mut chunk, &WithZone { event, tz })?
                    }
                }
                chunk.push(b'\n');
            }
            body.push(&chunk)?;
            chunk.clear();
            // 让同一个 worker 上排队的别的任务（收请求、回应答）插进来
            tokio::task::yield_now().await;
        }
        let (body, compressed) = body.finish()?;

        let sql = self.insert_sql();
        self.request(&sql, body, compressed).await?;

        tracing::debug!(
            count = events.len(),
            table = %self.table,
            format = self.insert_format.as_str(),
            "已写入 ClickHouse"
        );
        Ok(())
    }

    async fn healthcheck(&self) -> Result<()> {
        self.execute("SELECT 1").await?;

        let exists = self
            .execute(&format!(
                "EXISTS TABLE `{}`.`{}`",
                self.database, self.table
            ))
            .await?;
        if exists.trim() != "1" {
            return Err(Error::Sink(
                format!(
                    "表 {}.{} 不存在，执行 `metricpipe --ddl` 输出的语句建表",
                    self.database, self.table
                )
                .into(),
            ));
        }

        // 列齐不齐也要查。INSERT 带着 input_format_skip_unknown_fields=1，表里没有的
        // 字段不报错、整批也不失败，只是那个字段悄悄没了。启动时对一遍，缺了直接说清楚。
        let present = self
            .execute(&format!(
                "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}' FORMAT TSV",
                escape_literal(&self.database),
                escape_literal(&self.table)
            ))
            .await?;
        let present: std::collections::HashMap<&str, &str> = present
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(|line| match line.split_once('\t') {
                Some((name, ty)) => (name, ty.trim()),
                None => (line, ""),
            })
            .collect();
        let missing: Vec<String> = self
            .expected_columns()
            .into_iter()
            .filter(|name| !present.contains_key(name.as_str()))
            .collect();
        if !missing.is_empty() {
            return Err(Error::Sink(
                format!(
                    "表 {}.{} 缺列 {}：表结构没跟上配置，这些字段插入时会被静默丢掉。\
                     重跑 `metricpipe --ddl` 输出的语句（ddl Job）即可补齐",
                    self.database,
                    self.table,
                    missing.join(", ")
                )
                .into(),
            ));
        }

        // 属性列必须是 JSON。手建成 Map(String, String) 的话，`ADD COLUMN IF NOT EXISTS`
        // 见列名已存在会跳过，DDL 补不了类型；往 Map 列里插 JSON 对象倒是能成功（值会被
        // 转成字符串），但查询写法完全不同，所以这里当成错误，让人明确处理一次。
        let wrong_type: Vec<String> = ATTRIBUTE_COLUMNS
            .iter()
            .filter_map(|name| {
                let ty = present.get(name)?;
                let expected = if name.starts_with("exemplars.") {
                    format!("Array({ATTRIBUTES_TYPE})")
                } else {
                    ATTRIBUTES_TYPE.to_owned()
                };
                (!ty.is_empty() && !is_json_type(ty, &expected)).then(|| format!("{name} 是 {ty}"))
            })
            .collect();
        if !wrong_type.is_empty() {
            return Err(Error::Sink(
                format!(
                    "表 {}.{} 的属性列不是 JSON 类型（{}）。\
                     DROP（或 RENAME 保留老数据）之后重跑 `metricpipe --ddl` 建新表",
                    self.database,
                    self.table,
                    wrong_type.join("、")
                )
                .into(),
            ));
        }

        // 路径提示 / SKIP 只在 CREATE TABLE 时生效，老表上 `ADD COLUMN IF NOT EXISTS`
        // 见列已存在就跳过 —— 配了却没生效的话，查询照旧要写 `.:Int64`，而且人不会
        // 知道。不当错误（提示是优化，不是正确性），喊一声就够。
        for name in HINTED_COLUMNS {
            let Some(actual) = present.get(name) else {
                continue;
            };
            let absent: Vec<String> = self
                .attribute_hints()
                .filter(|hint| !actual.contains(hint.as_str()))
                .collect();
            if !absent.is_empty() {
                tracing::warn!(
                    column = name,
                    actual,
                    missing = %absent.join("、"),
                    "表里的属性列没带上配置的路径提示 / SKIP：这些只在建表时生效，\
                     老表要 MODIFY COLUMN（会重写整列）或者重建表才能补上"
                );
            }
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "clickhouse"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_lists_every_column_and_index() {
        let sink = ClickhouseSink::new("http://127.0.0.1:8123", "logs", "otel_metric");
        let ddl = sink.create_table_ddl();
        assert!(
            ddl.starts_with("CREATE TABLE IF NOT EXISTS `logs`.`otel_metric`"),
            "{ddl}"
        );
        assert!(
            ddl.contains("`timestamp`              DateTime64(9),"),
            "{ddl}"
        );
        assert!(
            ddl.contains("`exemplars.timestamp`    Array(DateTime64(9)),"),
            "{ddl}"
        );
        assert!(ddl.contains("`attributes`             JSON,"), "{ddl}");
        assert!(
            ddl.contains("`exemplars.attributes`   Array(JSON),"),
            "{ddl}"
        );
        for (name, _) in COLUMNS_HEAD
            .iter()
            .chain(COLUMNS_QUANTILES.iter())
            .chain(COLUMNS_TAIL.iter())
        {
            assert!(
                ddl.contains(&format!("`{name}`")),
                "DDL 少了 {name}:\n{ddl}"
            );
        }
        for (name, _) in INDEXES {
            assert!(ddl.contains(&format!("INDEX `{name}`")), "{ddl}");
            assert!(
                ddl.contains(&format!("ADD INDEX IF NOT EXISTS `{name}`")),
                "{ddl}"
            );
        }
        assert!(ddl.contains("ORDER BY (`service_name`, `metric_name`, toDateTime(`timestamp`))"));
        assert!(
            !ddl.contains("ADD COLUMN IF NOT EXISTS `timestamp`"),
            "{ddl}"
        );
        assert!(
            ddl.contains("ADD COLUMN IF NOT EXISTS `start_timestamp`"),
            "{ddl}"
        );
        assert!(!ddl.contains("MODIFY COLUMN"), "没配时区别去动列: {ddl}");
        // main 会在末尾补分号，这里不能自带
        assert!(!ddl.trim_end().ends_with(';'), "{ddl}");
    }

    /// 建表的列顺序、INSERT 的列清单、`CompactRow` 写值的顺序三者必须一致：紧凑格式
    /// 只按位置对，错一位就是把 `sum` 写进 `min`。
    #[test]
    fn table_columns_follow_the_fixed_column_order() {
        let sink = ClickhouseSink::new("http://127.0.0.1:8123", "logs", "otel_metric")
            .extra_columns(vec![
                ("cluster".to_owned(), "LowCardinality(String)".to_owned()),
                ("env".to_owned(), "LowCardinality(String)".to_owned()),
            ]);
        let base: Vec<String> = sink.base_columns().into_iter().map(|(n, _)| n).collect();
        assert_eq!(base, FIXED_COLUMNS);

        let columns = sink.insert_columns();
        assert_eq!(columns.len(), FIXED_COLUMNS.len() + 2);
        assert_eq!(&columns[FIXED_COLUMNS.len()..], ["cluster", "env"]);

        let sql = sink.insert_sql();
        assert!(
            sql.starts_with("INSERT INTO `logs`.`otel_metric` (`timestamp`, `start_timestamp`, "),
            "{sql}"
        );
        assert!(
            sql.contains(
                "`exemplars.attributes`, `flags`, `cluster`, `env`) FORMAT JSONCompactEachRow"
            ),
            "{sql}"
        );

        let plain = sink.insert_format(InsertFormat::JsonEachRow).insert_sql();
        assert_eq!(plain, "INSERT INTO `logs`.`otel_metric` FORMAT JSONEachRow");
    }

    #[test]
    fn json_type_check_accepts_parameters_and_rejects_map() {
        assert!(is_json_type("JSON", "JSON"));
        assert!(is_json_type("JSON(max_dynamic_paths=2048)", "JSON"));
        assert!(is_json_type("Array(JSON)", "Array(JSON)"));
        assert!(is_json_type(
            "Array(JSON(max_dynamic_paths=64))",
            "Array(JSON)"
        ));
        assert!(!is_json_type("Map(LowCardinality(String), String)", "JSON"));
        assert!(!is_json_type("Array(Map(String, String))", "Array(JSON)"));
        assert!(!is_json_type("JSON", "Array(JSON)"));
        assert!(!is_json_type("String", "JSON"));
    }

    #[test]
    fn timezone_goes_into_create_but_not_into_the_key_column() {
        let sink = ClickhouseSink::new("http://127.0.0.1:8123", "logs", "otel_metric")
            .timezone(chrono_tz::Asia::Shanghai);
        let ddl = sink.create_table_ddl();
        assert!(
            ddl.contains("`timestamp`              DateTime64(9, 'Asia/Shanghai'),"),
            "{ddl}"
        );
        assert!(
            ddl.contains("`start_timestamp`        DateTime64(9, 'Asia/Shanghai'),"),
            "{ddl}"
        );
        assert!(
            ddl.contains("MODIFY COLUMN `start_timestamp` DateTime64(9, 'Asia/Shanghai')"),
            "{ddl}"
        );
        assert!(
            ddl.contains(
                "MODIFY COLUMN `exemplars.timestamp` Array(DateTime64(9, 'Asia/Shanghai'))"
            ),
            "{ddl}"
        );
        // timestamp 是键列，ALTER 会被 ClickHouse 拒绝（ALTER_OF_COLUMN_IS_FORBIDDEN）
        assert!(
            !ddl.contains("MODIFY COLUMN `timestamp`"),
            "键列不能 MODIFY，整条 ALTER 都会失败: {ddl}"
        );
    }

    #[test]
    fn attribute_hints_and_skips_go_into_the_json_type() {
        let sink = ClickhouseSink::new("http://127.0.0.1:8123", "logs", "otel_metric")
            .attribute_types(vec![
                ("http.route".to_owned(), "String".to_owned()),
                ("http.response.status_code".to_owned(), "Int64".to_owned()),
            ])
            .attribute_skip(vec!["debug.payload".to_owned()])
            .attribute_skip_regexp(vec![r"^debug\..*".to_owned()]);
        let ddl = sink.create_table_ddl();

        let expect = "JSON(http.route String, http.response.status_code Int64, \
                      SKIP debug.payload, SKIP REGEXP '^debug\\\\..*')";
        // 列宽是按最长列名对齐的，别把空格个数写死
        let column_type = |name: &str| {
            ddl.lines()
                .find_map(|line| line.trim().strip_prefix(&format!("`{name}` ")))
                .unwrap_or_else(|| panic!("DDL 里没有 {name}:\n{ddl}"))
                .trim()
                .trim_end_matches(',')
                .to_owned()
        };
        assert_eq!(column_type("attributes"), expect);
        assert_eq!(column_type("resource_attributes"), expect);
        // exemplar 的属性不跟着建子列
        assert!(
            ddl.contains("`exemplars.attributes`   Array(JSON),"),
            "{ddl}"
        );
        // 老表补列时同样带上，虽然 ADD COLUMN 见列已存在会跳过（healthcheck 会 warn）
        assert!(
            ddl.contains(&format!("ADD COLUMN IF NOT EXISTS `attributes` {expect}")),
            "{ddl}"
        );
        // 正则里的反斜杠要按 SQL 字符串字面量转义
        assert!(ddl.contains(r"SKIP REGEXP '^debug\\..*'"), "{ddl}");
        // 没配就还是光秃秃的 JSON
        let plain = ClickhouseSink::new("http://127.0.0.1:8123", "logs", "otel_metric");
        assert!(plain
            .create_table_ddl()
            .contains("`attributes`             JSON,"));
    }

    #[test]
    fn cluster_ddl_is_replicated_plus_distributed() {
        let sink = ClickhouseSink::new("http://ck-lb:8123", "logs", "otel_metric")
            .cluster("bj_ck")
            .extra_columns(vec![(
                "cluster".to_owned(),
                "LowCardinality(String)".to_owned(),
            )]);
        let ddl = sink.create_table_ddl();
        assert!(
            ddl.contains("`logs`.`otel_metric_local` ON CLUSTER `bj_ck`"),
            "{ddl}"
        );
        assert!(ddl.contains("ENGINE = ReplicatedMergeTree"), "{ddl}");
        assert!(
            ddl.contains("Distributed(`bj_ck`, `logs`, `otel_metric_local`, cityHash64(`service_name`, `metric_name`))"),
            "{ddl}"
        );
        assert_eq!(ddl.matches("CREATE TABLE").count(), 2, "{ddl}");
        assert_eq!(ddl.matches("ALTER TABLE").count(), 2, "{ddl}");
        assert_eq!(ddl.matches(";\n").count(), 3, "{ddl}");
        // 索引只加在本地表上，Distributed 不支持跳数索引
        assert_eq!(
            ddl.matches("ADD INDEX IF NOT EXISTS `idx_metric_name`")
                .count(),
            1,
            "{ddl}"
        );
        assert_eq!(
            ddl.matches("ADD COLUMN IF NOT EXISTS `cluster`").count(),
            2,
            "{ddl}"
        );
        let local_alter = ddl.find("ALTER TABLE `logs`.`otel_metric_local`").unwrap();
        let dist_alter = ddl.find("ALTER TABLE `logs`.`otel_metric` ON").unwrap();
        assert!(
            local_alter < dist_alter,
            "先补本地表再补 Distributed 表:\n{ddl}"
        );
        assert!(
            ddl.contains("{shard}") && ddl.contains("{replica}"),
            "{ddl}"
        );
    }
}
