//! 打到标准输出，调试用。

use async_trait::async_trait;
use tokio::io::{AsyncWriteExt, Stderr, Stdout};

use crate::error::Result;
use crate::event::MetricEvent;
use crate::sink::Sink;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    /// 一行一个 JSON，和落库格式一致（时间戳按 UTC）。
    Json,
    /// 一行一个数据点的摘要：时间、服务、指标名、类型、值、属性。
    Text,
}

pub struct ConsoleSink {
    encoding: Encoding,
    target: Target,
}

enum Target {
    Stdout(Stdout),
    Stderr(Stderr),
}

impl ConsoleSink {
    pub fn new(encoding: Encoding) -> Self {
        Self {
            encoding,
            target: Target::Stdout(tokio::io::stdout()),
        }
    }

    pub fn stderr(mut self) -> Self {
        self.target = Target::Stderr(tokio::io::stderr());
        self
    }
}

impl Default for ConsoleSink {
    fn default() -> Self {
        Self::new(Encoding::Json)
    }
}

#[async_trait]
impl Sink for ConsoleSink {
    async fn write(&mut self, events: &[MetricEvent]) -> Result<()> {
        let mut buf: Vec<u8> = Vec::with_capacity(events.len() * 512);
        for event in events {
            match self.encoding {
                Encoding::Json => serde_json::to_writer(&mut buf, event)?,
                Encoding::Text => buf.extend_from_slice(event.to_text_line().as_bytes()),
            }
            buf.push(b'\n');
        }

        match &mut self.target {
            Target::Stdout(out) => {
                out.write_all(&buf).await?;
                out.flush().await?;
            }
            Target::Stderr(out) => {
                out.write_all(&buf).await?;
                out.flush().await?;
            }
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "console"
    }
}
