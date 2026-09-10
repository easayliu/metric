//! metricpipe —— 一个精简的 OTLP 指标采集入库框架，和 logpipe / tracepipe 同一套骨架。
//!
//! 数据流只有三段：
//!
//! ```text
//!   OTel SDK / collector ──OTLP──▶ Source ──(Batch)──▶ Pipeline ──(攒批 / 重试)──▶ Sink
//!                                  gRPC 4317 / HTTP 4318   批处理与背压              入库
//! ```
//!
//! * [`Source`] 负责收数据点（OTLP 接收端、读 stdin……），并在数据落库后收到 ack；
//! * [`Sink`] 负责把一批数据点写进存储（ClickHouse、控制台……）；
//! * [`Pipeline`] 负责攒批、重试、优雅退出，把两者串起来。
//!
//! 一个 OTLP 数据点就是一行记录：五种指标类型（Gauge / Sum / Histogram /
//! ExponentialHistogram / Summary）落在同一张表里，用 `metric_type` 区分，
//! 见 [`MetricEvent`]。
//!
//! ```no_run
//! use metricpipe::{Pipeline, sink::ClickhouseSink, source::OtlpSource};
//!
//! # async fn run() -> metricpipe::Result<()> {
//! Pipeline::builder()
//!     .source(OtlpSource::new())
//!     .sink(ClickhouseSink::new("http://127.0.0.1:8123", "logs", "otel_metric"))
//!     .build()?
//!     .run()
//!     .await
//! # }
//! ```

pub mod batch;
pub mod config;
pub mod error;
pub mod event;
pub mod otlp;
pub mod pipeline;
pub mod shutdown;
pub mod sink;
pub mod source;

pub use error::{Error, Result};
pub use event::{Exemplar, MetricEvent, MetricType, Quantile, Temporality};
pub use pipeline::{Pipeline, PipelineBuilder, RunningPipeline};
pub use shutdown::{Shutdown, ShutdownHandle};
pub use sink::Sink;
pub use source::{Batch, Source, SourceSender};
