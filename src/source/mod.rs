//! 接收端：产出数据点批次，并在数据成功落库后收到 ack。

pub mod otlp;
pub mod stdin;

pub use otlp::OtlpSource;
pub use stdin::StdinSource;

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

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
    /// 在队列里占的名额（按数据点数计），pipeline 把这批收进缓冲区时随 `Batch` 一起释放。
    pub(crate) permit: Option<OwnedSemaphorePermit>,
}

impl Batch {
    pub fn new(events: Vec<MetricEvent>) -> Self {
        Self {
            events,
            ack: None,
            permit: None,
        }
    }

    pub fn with_ack(events: Vec<MetricEvent>) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                events,
                ack: Some(tx),
                permit: None,
            },
            rx,
        )
    }
}

/// source 往下游发数据的入口。
///
/// 两层背压：通道按批数有界；另外还按**数据点总数**限额 —— 一个请求动辄上万个点，
/// 光数批数的话 64 批就能压进几十万个点、几百 MB 内存。名额在 [`Batch`] 里随身带着，
/// pipeline 把这批收进自己的缓冲区时释放。
#[derive(Clone, Debug)]
pub struct SourceSender {
    tx: mpsc::Sender<Batch>,
    limiter: Arc<Semaphore>,
    capacity: usize,
}

impl SourceSender {
    pub(crate) fn new(tx: mpsc::Sender<Batch>, limiter: Arc<Semaphore>, capacity: usize) -> Self {
        Self {
            tx,
            limiter,
            capacity,
        }
    }

    /// 给一批数据点占名额。一批比整个限额还大就按限额占满放行，否则永远等不到。
    async fn reserve(&self, events: usize) -> Result<OwnedSemaphorePermit> {
        let permits = events.clamp(1, self.capacity).min(u32::MAX as usize) as u32;
        Arc::clone(&self.limiter)
            .acquire_many_owned(permits)
            .await
            .map_err(|_| Error::other("下游已关闭"))
    }

    /// 发送一批数据点，不关心是否落库。
    pub async fn send(&self, events: Vec<MetricEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let permit = self.reserve(events.len()).await?;
        let mut batch = Batch::new(events);
        batch.permit = Some(permit);
        self.tx
            .send(batch)
            .await
            .map_err(|_| Error::other("下游已关闭"))
    }

    /// 发送一批数据点，返回的 receiver 会在落库成功后被唤醒；落库失败（重试耗尽）
    /// 时发送端被丢弃，receiver 会收到 `Err`。
    pub async fn send_with_ack(&self, events: Vec<MetricEvent>) -> Result<oneshot::Receiver<()>> {
        let permit = self.reserve(events.len()).await?;
        let (mut batch, ack) = Batch::with_ack(events);
        batch.permit = Some(permit);
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
