# metricpipe

一个精简的 OTLP 指标采集入库框架，和 [logpipe](../log)、[tracepipe](../trace) 同一套骨架
（source → pipeline → sink），只保留我们需要的部分：**接收应用上报的 OpenTelemetry 指标，
拍平成一行一个数据点，批量写进 ClickHouse。**

```text
  OTel SDK / collector    ┌────────────┐        ┌──────────────────────┐        ┌────────────┐
  ─ OTLP/gRPC :4317 ─────▶│   Source   │ Batch  │       Pipeline       │ 批量写  │    Sink    │
  ─ OTLP/HTTP :4318 ─────▶│ OTLP 接收端  ├───────▶│ 攒批 / 重试 / 背压     ├───────▶│ ClickHouse │
                          │ stdin      │        │ 优雅退出              │  ack   │ Console    │
                          └────────────┘        └──────────────────────┘◀───────└────────────┘
                                ▲                                         wait_for_write 时
                                └── 队列满回 UNAVAILABLE / 503，SDK 自己重发      落库后才回成功
```

三兄弟的分工：logpipe 采日志、tracepipe 采 trace、metricpipe 采指标。指标里的 exemplar 带着
`trace_id`（同样是 32 位小写 hex），面板上看到一个尖峰，用它直接跳到 tracepipe 的表里那次请求，
再用同一个 id 去 logpipe 的表里翻日志。

## 数据模型

OTLP 里一个导出请求是 `ResourceMetrics → ScopeMetrics → Metric → DataPoint` 四层，落库时拍平：
**每个数据点一行**，上面几层的信息复制到每一行上。

五种指标类型（`Gauge` / `Sum` / `Histogram` / `ExponentialHistogram` / `Summary`）落在**同一张
表**里，用 `metric_type` 区分，各自只填得上的那几列，用不上的留默认值（数值 0、数组空）。
OTel collector 的 [clickhouse exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter)
是分五张表的，这里合成一张：一条 pipeline 只有一个 sink、一张表，而且「这个服务的所有指标」
不用 union 五张表。代价是一行里有二十来列对当前类型没意义 —— 列式存储里空列不占什么空间。

| 列 | 来源 | 说明 |
| --- | --- | --- |
| `timestamp` | `time_unix_nano` | 数据点时间，纳秒精度 |
| `start_timestamp` | `start_time_unix_nano` | 这一段累计的起点；没上报是 0 |
| `metric_name` / `metric_unit` / `metric_description` | `Metric` | 比如 `http.server.request.duration` / `ms` |
| `metric_type` | `Metric.data` | `Gauge` / `Sum` / `Histogram` / `ExponentialHistogram` / `Summary` |
| `service_name` | resource 的 `service.name` | 没有则 `unknown_service` |
| `scope_name` / `scope_version` | scope | 比如 `io.opentelemetry.runtime-telemetry` / `2.9.0` |
| `resource_attributes` | resource | `JSON`，每个 key 一个带类型的子列，见下面「属性列」 |
| `attributes` | 数据点 | `JSON`，这条时间线的标签（`http.route`、`jvm.memory.type`……） |
| `value` | Gauge / Sum | 整数上报的（`asInt`）也转成 f64 装在这一列 |
| `temporality` | Sum / Histogram | `Delta` / `Cumulative` / `Unspecified`，算 rate 要看它 |
| `is_monotonic` | Sum | 1 = counter，0 = up-down counter |
| `count` / `sum` / `min` / `max` | Histogram / Summary | Summary 没有 min / max |
| `bucket_counts` / `explicit_bounds` | Histogram | 桶计数比上界多一个（最后一个是上界之外的） |
| `scale` / `zero_count` / `zero_threshold` / `positive_*` / `negative_*` | ExponentialHistogram | 指数桶，base = 2^(2^-scale) |
| `quantiles.*` | Summary | `Nested`：`quantile` / `value` 两个等长数组 |
| `exemplars.*` | 数据点 exemplars | `Nested`：`timestamp` / `value` / `trace_id` / `span_id` / `attributes` |
| `flags` | `flags` | bit 0 = 这条时间线没数据（Prometheus 的 staleness marker） |

属性保留 OTLP 里的类型：字符串、整数、小数、布尔原样，bytes 转 base64 串，数组和嵌套对象就是
JSON 数组 / 对象，没有值的属性记 `null`。

**没上报和 0 分不开**：OTLP 里 `sum` / `min` / `max` 是可选的，没上报一律落成 0。要区分的话看
`count`（0 表示这个周期没有样本）和 `flags`。

**NaN / Inf 落成 0**：JSON 里没有这几个字面量的写法，序列化时会变成 `null`，INSERT 带着
`input_format_null_as_default=1` 收成列的默认值。不这么做的话一个 staleness marker 就能让整批
插入失败。

### 属性列

三个属性列（`resource_attributes` / `attributes` / `exemplars.attributes`）是 ClickHouse 的
`JSON` 类型，**需要 ClickHouse 25.3+**（JSON 类型在 25.3 转正）。用 JSON 而不是
`Map(String, String)`：Map 查一个 key 要把整个 map 列解压出来再取值；JSON 列里每个 key 是独立的
子列，查 `attributes.http.route` 只读这一个子列，而且值带类型。

值得记住的一条：**路径数别失控**。超过 `max_dynamic_paths`（默认 1024）的路径会掉进 shared data，
还能查但会退化（25.8 起的 `map_with_buckets` / `advanced` 序列化就是在救这种场景，代价是
`advanced` 要多存一份）。指标的标签本来就该低基数，正常碰不到这条线 —— 真碰到了，说明有人拿
pod 名或者请求 id 当标签了，该治的是那边。

写法要点（和 tracepipe 完全一样）：

* **key 里的点就是路径分隔**：`http.response.status_code` 存成 `http` → `response` →
  `status_code`，查询写 `attributes.http.response.status_code`。
* **同一个 key 类型不一致时按类型取**：一个服务发整数、另一个发字符串 `"200"`，直接 `= 500` 会
  报 `NO_COMMON_TYPE`。写 `attributes.http.response.status_code.:Int64 = 500` 只取整数那部分，
  或者 `toString(...) = '500'` 两边都要。
* **已知的 key 给类型提示**（`sink.attribute_types`）：写进配置后 `--ddl` 会把属性列建成
  `JSON(http.route String, http.response.status_code Int64)`。带提示的路径物化成**类型确定**的
  独立子列，查询直接写 `attributes.http.route`（不用 `.:String`），而且不同服务对同一个 key 发
  不同类型时也不会再 `NO_COMMON_TYPE`。指标的标签 key 多半来自语义约定，类型是定死的，值得提示。
  不想要的 key 用 `sink.attribute_skip`（精确路径）和 `sink.attribute_skip_regexp`（正则）挡掉，
  连子列都不建。两者都只作用在 `resource_attributes` / `attributes` 上，exemplar 的属性不跟着建。
  两列用的是同一份提示，所以一个只出现在 resource 上的 key（`k8s.pod.name`）在 `attributes` 那边
  会是一个全空的子列 —— 稀疏序列化下几乎不占东西，但也别为此把不查的 key 全列上。

  **只在建表时生效**：老表上 `ADD COLUMN IF NOT EXISTS` 见列已存在会跳过，要生效得
  `MODIFY COLUMN`（重写整列）或者重建表。配了却没生效的话 healthcheck 会 warn 一句，不会拦着启动。
* **跳数索引建在带类型的子列上**：`--ddl` 不预建属性索引，哪些 key 值得建由查询决定，自己加：

  ```sql
  ALTER TABLE logs.otel_metric
      ADD INDEX idx_http_route attributes.http.route.:String TYPE bloom_filter(0.01) GRANULARITY 1;
  ```

  直接在 `attributes.http.route` 上建会被拒（`Unexpected type Dynamic of bloom filter index`），
  必须带 `.:类型`。历史 part 要 `MATERIALIZE INDEX` 才有。
* **路径数上限**：JSON 列默认最多 1024 个不同路径，超过的进「共享数据」区，还能查但退化成整列
  解析。指标的标签 key 数量有限，一般够；确认要更多就改 `--ddl` 输出里的类型：
  `` `attributes` JSON(max_dynamic_paths = 4096) ``，healthcheck 认带参数的写法。**别把 pod 名、
  用户 id 这种高基数的东西当标签**——那是时间线爆炸，不只是列数的问题。

**时间戳一律带偏移写入**（`2026-09-07 11:04:08.914293456+08:00`）：数据点的时间本来就是绝对
时刻，存进去的时刻不依赖列有没有标时区。`sink.timezone` 只影响 `--ddl` 建出来的列类型
（`DateTime64(9, 'Asia/Shanghai')`），也就是查出来显示成几点。和 logpipe / tracepipe 的表配成
同一个时区，几张表对照时才是同一口径的时间。

## 启动

有两种用法：直接跑二进制（读 YAML 配置），或者当库用（拓扑写在代码里）。

```bash
cp metricpipe.yaml /etc/metricpipe.yaml     # 仓库根目录有带注释的示例配置
cargo run --release -- --ddl /etc/metricpipe.yaml | clickhouse-client   # 建表
cargo run --release -- --check /etc/metricpipe.yaml                     # 只校验配置
cargo run --release -- /etc/metricpipe.yaml                             # 启动
RUST_LOG=debug cargo run -- /etc/metricpipe.yaml                        # 看每批收发
```

不带参数时读当前目录的 `metricpipe.yaml`。`Ctrl-C` / SIGTERM 是优雅退出：停止收新请求、
手上的数据先写完再退。

最小配置（先用 console sink 看收到的数据点，不用连库）：

```yaml
source:
  type: otlp        # gRPC 0.0.0.0:4317 + HTTP 0.0.0.0:4318，都是 SDK 的默认端口

sink:
  type: console
  encoding: json    # 或 text：一行一个数据点的摘要
```

然后把应用指过来，Java agent 的话：

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://<metricpipe>:4317 \
OTEL_SERVICE_NAME=order-service \
OTEL_METRIC_EXPORT_INTERVAL=60000 \
java -javaagent:opentelemetry-javaagent.jar -jar app.jar
```

换成入库只要改 `sink`：

```yaml
sink:
  type: clickhouse
  endpoint: http://127.0.0.1:8123
  database: logs
  table: otel_metric
  timezone: Asia/Shanghai
  user: default
  password: ""

fields:            # 附加到每个数据点的静态字段
  cluster: bj-prod
  env: prod
```

完整可配项（`source` / `sink` / `batch` / `retry` / `pipeline` / `fields`）见 `metricpipe.yaml`
里的注释；写错的键会在启动时直接报错，不会静默忽略。

### 接收端的行为

| 项 | 默认 | 说明 |
| --- | --- | --- |
| `grpc` / `http` | `0.0.0.0:4317` / `0.0.0.0:4318` | 写 `null` 或空串关掉其中一个；两个都关启动报错 |
| 编码 | protobuf + JSON | HTTP 按 `Content-Type` 分派（`application/x-protobuf` / `application/json`）；gRPC 只有 protobuf |
| 压缩 | gzip | 两个入口都收 `gzip`（SDK 的 `OTEL_EXPORTER_OTLP_COMPRESSION=gzip`），解压后仍按大小上限卡 |
| `max_request_bytes` | 16 MiB | 单个请求（解压后）的上限，超过回 `PAYLOAD_TOO_LARGE` |
| `enqueue_timeout_secs` | 5 | 下游队列满时最多等多久，超时回 gRPC `UNAVAILABLE` / HTTP `503 + Retry-After` |
| `wait_for_write` | `false` | 见下面「投递语义」 |

拒收一律回「可重试」的状态码，这是 OTLP 规定的应答方式：SDK 自带指数退避重发，比把数据堆在
接收端内存里稳。坏请求（解析不了、不认的 Content-Type）回 400 / 415，SDK 不会重发。

OTLP/HTTP + JSON 有个坑，值得单说：`Metric.data` 在 opentelemetry-proto 里是个
`#[serde(flatten)]` 的 oneof，**底下任何一处解不出来，整个 metric 的 data 会静默变成
`None`** —— 不报错，就是一整个指标凭空消失。而它的 serde 实现和 protobuf 的 JSON 映射有
三处对不齐：64 位整数（`asInt`）按规范编成字符串但 oneof 上没挂 deserializer；
`Exemplar` / `Buckets` / `ValueAtQuantile` 和指数直方图的数据点没有 `#[serde(default)]`，
而 proto3 的 JSON 会省掉零值字段；exemplar 的值是个没 flatten 的 oneof。所以接收端解 JSON
前会先按 OTLP 的结构补一道（`src/otlp.rs` 的 `normalize`），解完还会对一下数据点个数，
少了就 `warn`。protobuf 那条路（gRPC 和 `application/x-protobuf`）不经过这些，也没这些问题。

## 当库用

```rust
use metricpipe::{Pipeline, sink::ClickhouseSink, source::OtlpSource};

#[tokio::main]
async fn main() -> metricpipe::Result<()> {
    Pipeline::builder()
        .source(OtlpSource::new().wait_for_write(true))
        .sink(
            ClickhouseSink::new("http://127.0.0.1:8123", "logs", "otel_metric")
                .timezone(chrono_tz::Asia::Shanghai),
        )
        .require_healthy(true)
        .build()?
        .run()          // 跑到 Ctrl-C；手上的数据会先写完再退出
        .await
}
```

`examples/` 下有两个直接跑的例子：

```bash
cargo run --example otlp_to_console -- text
cargo run --example otlp_to_clickhouse
```

## 表结构

字段由程序定义（上面那些，外加 `fields` 里的静态字段），**建表由你自己执行**，采集进程不执行
任何 DDL —— 分区键、排序键、TTL、引擎这些线上细节留在你手里。启动时只做校验：
`require_healthy: true` 的情况下会 `SELECT 1` + `EXISTS TABLE` + 对一遍 `system.columns`，
表不存在或者**缺列就直接报错退出**，报错里写清缺哪几列、怎么补。

```bash
cargo run -- --ddl metricpipe.yaml | clickhouse-client
```

```sql
CREATE TABLE IF NOT EXISTS `logs`.`otel_metric`
(
    `timestamp`              DateTime64(9, 'Asia/Shanghai'),
    `start_timestamp`        DateTime64(9, 'Asia/Shanghai'),
    `metric_name`            LowCardinality(String),
    `metric_type`            LowCardinality(String),
    `metric_unit`            LowCardinality(String),
    `metric_description`     String,
    `service_name`           LowCardinality(String),
    `scope_name`             LowCardinality(String),
    `scope_version`          LowCardinality(String),
    `resource_attributes`    JSON,
    `attributes`             JSON,
    `value`                  Float64,
    `temporality`            LowCardinality(String),
    `is_monotonic`           UInt8,
    `count`                  UInt64,
    `sum`                    Float64,
    `min`                    Float64,
    `max`                    Float64,
    `bucket_counts`          Array(UInt64),
    `explicit_bounds`        Array(Float64),
    `scale`                  Int32,
    `zero_count`             UInt64,
    `zero_threshold`         Float64,
    `positive_offset`        Int32,
    `positive_bucket_counts` Array(UInt64),
    `negative_offset`        Int32,
    `negative_bucket_counts` Array(UInt64),
    `quantiles.quantile`     Array(Float64),
    `quantiles.value`        Array(Float64),
    `exemplars.timestamp`    Array(DateTime64(9, 'Asia/Shanghai')),
    `exemplars.value`        Array(Float64),
    `exemplars.trace_id`     Array(String),
    `exemplars.span_id`      Array(String),
    `exemplars.attributes`   Array(JSON),
    `flags`                  UInt32,
    `cluster`                LowCardinality(String),
    INDEX `idx_metric_name` `metric_name` TYPE bloom_filter GRANULARITY 4,
    INDEX `idx_exemplar_trace_id` `exemplars.trace_id` TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toDate(`timestamp`)
ORDER BY (`service_name`, `metric_name`, toDateTime(`timestamp`))
TTL toDateTime(`timestamp`) + INTERVAL 30 DAY;

ALTER TABLE `logs`.`otel_metric`
    ADD COLUMN IF NOT EXISTS `start_timestamp` DateTime64(9, 'Asia/Shanghai'),
    ...
    ADD INDEX IF NOT EXISTS `idx_metric_name` `metric_name` TYPE bloom_filter GRANULARITY 4,
    ...
    MODIFY COLUMN `exemplars.timestamp` Array(DateTime64(9, 'Asia/Shanghai'));
```

两段：`CREATE TABLE IF NOT EXISTS` 管新表，后面的 `ALTER TABLE` 管老表 —— 全是 `IF NOT EXISTS`
这类幂等操作，新表上跑是空转，老表上跑就把差异补齐。所以**表结构变了重跑一遍就行**。

唯一补不了的是 `timestamp` 列的时区：它在排序键和分区键里，ClickHouse 不允许 ALTER 键列
（`ALTER_OF_COLUMN_IS_FORBIDDEN`），所以 ALTER 段只 MODIFY `start_timestamp` 和
`exemplars.timestamp`。建表时没配 `timezone` 的老表，`timestamp` 就一直是裸 `DateTime64(9)`：
存的时刻不受影响（INSERT 一律带偏移），只是查出来按服务端时区显示。要换显示时区只能重建表，
或者查询时 `toTimeZone(timestamp, 'Asia/Shanghai')`。

要点：

* 排序键是 `(service_name, metric_name, toDateTime(timestamp))`：面板几乎都是「某个服务的某个
  指标，最近一段时间」，同一条时间线的点挨着放，`value` 那几列的压缩率也跟着上去。只按指标名
  查（不给 service）靠 `idx_metric_name` 跳 granule。
* `quantiles.*` / `exemplars.*` 是 ClickHouse 的 `Nested` 平铺写法，插入时按几个等长数组给。
* `fields` 里的静态字段类型按值推断：字符串 → `LowCardinality(String)`、整数 → `Int64`、
  小数 → `Float64`、布尔 → `UInt8`。**给线上配置新加了 `fields`，重跑一次 `--ddl` 的输出
  （ddl Job）**，忘了的话启动时 healthcheck 会点名缺哪几列。
* 集群写法和另外两个项目一样：`sink.cluster` 填 ClickHouse 集群名，`--ddl` 生成
  `ReplicatedMergeTree` 本地表 `otel_metric_local` + `Distributed` 表 `otel_metric`，全部
  `ON CLUSTER`。分片键是 `cityHash64(service_name, metric_name)`：同一条时间线始终落同一个
  分片，按时间线聚合不用跨分片。
* 指标量大且只看趋势的话，可以在这张表上再挂一个物化视图按分钟预聚合（`AggregatingMergeTree`）
  —— 程序不关心，DDL 是你的。

### 常用查询

```sql
-- 某个服务某个指标最近一小时的曲线（Gauge / Sum 看 value）
select toStartOfMinute(timestamp) as t, avg(value)
from logs.otel_metric
where service_name = 'order-service' and metric_name = 'jvm.memory.used'
  and attributes.jvm.memory.type.:String = 'heap'
  and timestamp > now() - interval 1 hour
group by t order by t;

-- counter 的每秒增量：Cumulative 要自己做差分
select t, greatest(0, v - lagInFrame(v) over (order by t)) / 60 as per_sec
from (
    select toStartOfMinute(timestamp) as t, max(value) as v
    from logs.otel_metric
    where service_name = 'order-service' and metric_name = 'http.server.request.count'
      and temporality = 'Cumulative' and timestamp > now() - interval 6 hour
    group by t
) order by t;

-- Histogram 估分位：累加桶找 p99 落在哪个上界
select metric_name,
       arraySum(bucket_counts) as total,
       explicit_bounds[arrayFirstIndex(x -> x >= total * 0.99, arrayCumSum(bucket_counts))] as p99_le
from logs.otel_metric
where service_name = 'order-service' and metric_name = 'http.server.request.duration'
  and timestamp > now() - interval 5 minute
group by metric_name, bucket_counts, explicit_bounds;

-- 从指标跳 trace：exemplar 带着 trace_id，走 idx_exemplar_trace_id
select timestamp, metric_name, exemplars.value, exemplars.trace_id
from logs.otel_metric
where length(exemplars.trace_id) > 0
  and metric_name = 'http.server.request.duration'
  and timestamp > now() - interval 10 minute
order by arrayMax(exemplars.value) desc limit 20;

-- 反过来：拿一个 trace id 看它被哪些指标采样到
select timestamp, service_name, metric_name
from logs.otel_metric
where has(exemplars.trace_id, 'e89a476882236ce0f1186d1522c8f59f');

-- 某个服务上报了哪些指标 / 哪些标签 key
select metric_name, metric_type, count() from logs.otel_metric
where service_name = 'order-service' and timestamp > now() - interval 1 hour
group by metric_name, metric_type order by count() desc;

select arrayJoin(JSONAllPaths(attributes)) as k, count() from logs.otel_metric
where service_name = 'order-service' and timestamp > now() - interval 1 hour
group by k order by count() desc;
```

### 接 Grafana

ClickHouse 数据源的 Time series 模式：查询给出 `time` 列和一个数值列即可，上面那几条
`group by toStartOfMinute(...)` 的写法直接能用。指标 → trace 用 Data links / Exemplars：
把 `exemplars.trace_id` 展开成一列，链接指到 tracepipe 的数据源。

## 投递语义

* **默认（`wait_for_write: false`）**：请求进了队列就给 SDK 回成功，和 OTel collector 的默认行为
  一样。ClickHouse 写失败按指数退避重试（默认 5 次），仍失败时 `on_error: stop` 停机、
  `drop` 丢掉继续 —— 都会丢这一批。
* **`wait_for_write: true`**：等数据**真正写进存储**再回成功。写失败会反映成 SDK 那边的导出失败，
  SDK 自己重发，等于没有磁盘缓冲也有「至少一次」。代价是每个导出请求多等一个攒批周期
  （`batch.timeout_secs`）加一次写入的时间，SDK 的导出超时（默认 10s）要比这个长。
* 背压：source 与 sink 之间是有界队列（`pipeline.buffer`），存储慢下来时新请求会在队列口等
  `enqueue_timeout_secs`，等不到就回「稍后重试」，不会把内存吃光。
* 写入可能被重试，所以同一批数据点可能重复入库（表是 MergeTree，重复行不会合并）。
  指标是 Cumulative 的话重复行对 `max` / 差分没影响；确实要去重就换 `ReplacingMergeTree`
  并把时间线的标识加进排序键 —— 改 `--ddl` 输出的 SQL 就行，程序不关心。

## 部署（k8s）

push 模型：应用主动发过来，所以是 Deployment + Service，不是 DaemonSet；不读宿主机文件，
不需要 root，也不需要 RBAC。`deploy/metricpipe-deployment.yaml` 可以直接 apply。

**顺序是先建表、再起 Deployment** —— 配了 `require_healthy: true`，表不存在时 healthcheck 直接
失败退出，Pod 会 CrashLoopBackOff：

```bash
# 1. namespace + ConfigMap + Deployment + Service
kubectl apply -f deploy/metricpipe-deployment.yaml

# 2. 建库建表（挂的是同一个 ConfigMap，列不会和采集端对不上）
kubectl apply -f deploy/metricpipe-ddl-job.yaml
kubectl -n monitoring wait --for=condition=complete job/metricpipe-ddl --timeout=180s

# 3. 让第 1 步已经起来的 Pod 立刻重试，不用等 CrashLoop 退避
kubectl -n monitoring rollout restart deployment/metricpipe
```

然后应用的 `OTEL_EXPORTER_OTLP_ENDPOINT` 指到
`http://metricpipe.monitoring.svc.cluster.local:4317`。

Job 里 `apply-ddl` 容器的 `CH_HOST` / `CH_DATABASE` / `CH_CLUSTER` / `CH_USER` / `CH_PASSWORD`
要和 ConfigMap 里 sink 的对上。**改了配置里的 `fields` 或 `timezone` 就重跑一次 Job**。

不想在集群里跑 Job 的话，`--ddl` 不连库，本地也能渲染：

```bash
kubectl -n monitoring get cm metricpipe-config -o jsonpath='{.data.metricpipe\.yaml}' > /tmp/metricpipe.yaml
docker run --rm -v /tmp/metricpipe.yaml:/etc/metricpipe/metricpipe.yaml:ro \
  ghcr.io/easayliu/metric:v0.1.0 --ddl /etc/metricpipe/metricpipe.yaml
```

要点：多副本各自小批量写，`async_insert: true` 让 ClickHouse 服务端再攒一层；
`terminationGracePeriodSeconds` 留够（滚动更新时正在等 `wait_for_write` 的请求要写完）；
Service 前面走 gRPC 的话注意 k8s Service 是按连接负载均衡的，一个 SDK 的长连接只打到一个副本 ——
副本数按「够用」配，不是按均摊算。

### 发布

镜像由 CI 构建推送，打 tag 就发版（tag 必须单独推）：

```bash
# 1. 先改 Cargo.toml 的 version，CI 会校验它和 tag 一致
git commit -am "release v0.1.0"
git push origin main

# 2. tag 单独推，不能和分支挤在同一条 git push 里，否则不触发构建
git tag v0.1.0
git push origin v0.1.0
```

产出 `ghcr.io/easayliu/metric:v0.1.0`，同时把 `:latest` 指过去。`.github/workflows/ci.yml` 在
push / PR 上跑 `fmt --check` + `clippy -D warnings` + `cargo test`；`docker.yml` 构建前复用它
作为闸门。

## 调试：回放 OTLP/JSON

`type: stdin` 读 OTLP/JSON，一行一个导出请求，正是 OTel collector `file` exporter 写出来的格式：

```yaml
source:
  type: stdin
sink:
  type: console
  encoding: text
```

```bash
cat metrics.jsonl | metricpipe stdin.yaml
```

## 加自己的组件

只有两个 trait，都很短：

```rust
#[async_trait]
pub trait Source: Send + 'static {
    async fn run(self: Box<Self>, out: SourceSender, shutdown: Shutdown) -> Result<()>;
}

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    async fn write(&mut self, events: &[MetricEvent]) -> Result<()>;
    async fn healthcheck(&self) -> Result<()> { Ok(()) }
}
```

攒批、重试、ack、退出都在 pipeline 里。内置组件：

| 类型 | 组件 | 说明 |
| --- | --- | --- |
| source | `OtlpSource` | OTLP/gRPC + OTLP/HTTP 接收端，gzip、大小上限、队列背压 |
| source | `StdinSource` | 读 OTLP/JSON 行，调试 / 回放用 |
| sink | `ClickhouseSink` | HTTP `JSONEachRow` 批量插入，gzip 请求体 |
| sink | `ConsoleSink` | JSON / 摘要文本输出 |
| sink | `MemorySink` | 测试用 |

## 还没做

* 只收指标：OTLP 的 logs / traces 信号没有接（分别走 logpipe / tracepipe）
* 不主动拉：没有 Prometheus scrape / remote write 入口，靠 SDK 或 collector 推过来
* 不做聚合和降采样：收到什么存什么，预聚合交给物化视图，过期交给 TTL
* Delta / Cumulative 的相互转换（收到什么口径存什么口径，查询时自己处理）
* TLS / 鉴权：接收端是明文，只应暴露在集群内
* ClickHouse 多 endpoint 轮询 / 故障转移（现在只能填一个地址，靠外面的 LB）
* 磁盘缓冲（`wait_for_write` 让 SDK 兜底，代价是延迟）
* 采集自身指标（收发条数 / 拒收次数暂时只有 tracing 日志）
