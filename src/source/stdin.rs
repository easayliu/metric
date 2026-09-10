//! 从标准输入读 OTLP/JSON：一行一个 `ExportMetricsServiceRequest`，正是 OTel collector
//! `file` exporter 写出来的格式。调试解析、回放历史数据时用：
//!
//! ```bash
//! cat metrics.jsonl | metricpipe stdin.yaml
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::error::Result;
use crate::event::MetricEvent;
use crate::otlp;
use crate::shutdown::Shutdown;
use crate::source::{Source, SourceSender};

pub struct StdinSource {
    batch_lines: usize,
    flush_interval: Duration,
    fields: Option<Arc<BTreeMap<String, Value>>>,
}

impl StdinSource {
    pub fn new() -> Self {
        Self {
            batch_lines: 500,
            flush_interval: Duration::from_millis(500),
            fields: None,
        }
    }

    /// 攒多少行（请求）发一批。
    pub fn batch_lines(mut self, batch_lines: usize) -> Self {
        self.batch_lines = batch_lines.max(1);
        self
    }

    /// 附加到每条数据点 上的静态字段。
    pub fn fields(mut self, fields: BTreeMap<String, Value>) -> Self {
        self.fields = (!fields.is_empty()).then(|| Arc::new(fields));
        self
    }
}

impl Default for StdinSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for StdinSource {
    async fn run(self: Box<Self>, out: SourceSender, shutdown: Shutdown) -> Result<()> {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        let mut pending: Vec<MetricEvent> = Vec::new();
        let mut lines_in_batch = 0usize;
        let mut line_no = 0usize;
        let mut ticker = tokio::time::interval(self.flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                line = lines.next_line() => match line? {
                    Some(line) => {
                        line_no += 1;
                        if line.trim().is_empty() {
                            continue;
                        }
                        match otlp::decode_json(&line) {
                            Ok(request) => {
                                pending.extend(otlp::convert(request, self.fields.as_ref()));
                                lines_in_batch += 1;
                            }
                            Err(err) => {
                                tracing::warn!(line = line_no, %err, "跳过解析失败的行");
                            }
                        }
                        if lines_in_batch >= self.batch_lines {
                            out.send(std::mem::take(&mut pending)).await?;
                            lines_in_batch = 0;
                        }
                    }
                    None => break, // EOF
                },
                _ = ticker.tick() => {
                    // 交互式输入时定期收口，别让最后几行一直等不到发出去
                    if !pending.is_empty() {
                        out.send(std::mem::take(&mut pending)).await?;
                        lines_in_batch = 0;
                    }
                }
            }
        }

        out.send(pending).await?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "stdin"
    }
}
