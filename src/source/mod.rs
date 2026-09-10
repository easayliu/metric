//! 接收端：产出数据点批次，并在数据成功落库后收到 ack。

pub mod otlp;
pub mod stdin;

pub use otlp::OtlpSource;
pub use stdin::StdinSource;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use crate::error::{Error, Result};
use crate::event::MetricEvent;
use crate::shutdown::Shutdown;

/// 一批数据点，可选携带一个 ack 通道。
///
/// pipeline 会在这批数据**确实写进存储之后**才回 ack。OTLP source 配了
/// `wait_for_write` 时靠它决定什么时候给客户端回成功，从而做到「至少一次」投递。
pub struct Batch {
    pub events: Vec<MetricEvent>,
    pub(crate) ack: Option<oneshot::Sender<()>>,
}

impl Batch {
    pub fn new(events: Vec<MetricEvent>) -> Self {
        Self { events, ack: None }
    }

    pub fn with_ack(events: Vec<MetricEvent>) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                events,
                ack: Some(tx),
            },
            rx,
        )
    }
}

/// source 往下游发数据的入口。channel 有界，天然形成背压。
#[derive(Clone, Debug)]
pub struct SourceSender {
    tx: mpsc::Sender<Batch>,
}

impl SourceSender {
    pub(crate) fn new(tx: mpsc::Sender<Batch>) -> Self {
        Self { tx }
    }

    /// 发送一批数据点，不关心是否落库。
    pub async fn send(&self, events: Vec<MetricEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        self.tx
            .send(Batch::new(events))
            .await
            .map_err(|_| Error::other("下游已关闭"))
    }

    /// 发送一批数据点，返回的 receiver 会在落库成功后被唤醒；落库失败（重试耗尽）
    /// 时发送端被丢弃，receiver 会收到 `Err`。
    pub async fn send_with_ack(&self, events: Vec<MetricEvent>) -> Result<oneshot::Receiver<()>> {
        let (batch, ack) = Batch::with_ack(events);
        self.tx
            .send(batch)
            .await
            .map_err(|_| Error::other("下游已关闭"))?;
        Ok(ack)
    }
}

#[async_trait]
pub trait Source: Send + 'static {
    /// 持续产出数据点，直到数据读完或收到退出信号。
    async fn run(self: Box<Self>, out: SourceSender, shutdown: Shutdown) -> Result<()>;

    fn name(&self) -> &'static str {
        "source"
    }
}
