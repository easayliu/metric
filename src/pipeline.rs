//! 把 source 和 sink 串起来：攒批、重试、优雅退出。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::batch::{BatchConfig, RetryConfig};
use crate::error::{Error, Result};
use crate::event::MetricEvent;
use crate::shutdown::{self, Shutdown, ShutdownHandle};
use crate::sink::Sink;
use crate::source::{Source, SourceSender};

/// 逐条改写事件；返回 `None` 表示丢弃这条数据点。
pub type Transform = Box<dyn FnMut(MetricEvent) -> Option<MetricEvent> + Send>;

/// 一批数据重试耗尽后怎么办。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnError {
    /// 停掉整条 pipeline。默认。配了 `wait_for_write` 的话，还在等回执的客户端会收到
    /// 失败并重发；没配的话这批就丢了。
    Stop,
    /// 丢掉这批继续跑。**会丢数据**，只适合可容忍丢失的场景。
    Drop,
}

pub struct Pipeline {
    source: Box<dyn Source>,
    sink: Box<dyn Sink>,
    transforms: Vec<Transform>,
    batch: BatchConfig,
    retry: RetryConfig,
    buffer: usize,
    max_queued_events: usize,
    stats_interval: Option<Duration>,
    require_healthy: bool,
    on_error: OnError,
}

/// 默认最多让多少个数据点排在 source 与 pipeline 之间的队列里。
///
/// 一个点在内存里连属性带结构体约 1 KB，10 万个点约 100 MB；再加上正在攒的一批和
/// 正在写的一批（各 `batch.max_events`），就是这个进程的内存上界。
pub const DEFAULT_MAX_QUEUED_EVENTS: usize = 100_000;

/// 默认每 30 秒打一条运行统计。
pub const DEFAULT_STATS_INTERVAL: Duration = Duration::from_secs(30);

impl Pipeline {
    pub fn builder() -> PipelineBuilder {
        PipelineBuilder::default()
    }

    /// 前台运行，直到 source 结束、出错，或者收到退出信号（SIGINT / SIGTERM）。
    pub async fn run(self) -> Result<()> {
        let (handle, shutdown) = shutdown::channel();
        let mut task = tokio::spawn(self.run_inner(shutdown));

        tokio::select! {
            result = &mut task => flatten(result),
            signal = terminate_signal() => {
                tracing::info!(signal, "收到退出信号，开始收尾");
                handle.trigger();
                flatten(task.await)
            }
        }
    }

    /// 后台运行，返回的句柄可以随时停。
    pub fn spawn(self) -> RunningPipeline {
        let (handle, shutdown) = shutdown::channel();
        RunningPipeline {
            handle,
            task: tokio::spawn(self.run_inner(shutdown)),
        }
    }

    async fn run_inner(self, shutdown: Shutdown) -> Result<()> {
        let Self {
            source,
            sink,
            mut transforms,
            batch,
            retry,
            buffer,
            max_queued_events,
            stats_interval,
            require_healthy,
            on_error,
        } = self;

        if let Err(err) = sink.healthcheck().await {
            if require_healthy {
                return Err(err);
            }
            tracing::warn!(sink = sink.name(), %err, "healthcheck 未通过，仍然继续启动");
        }

        // 队列两道闸：按批数（通道容量）和按数据点总数（信号量）。后者才是内存上界，
        // 前者只是防止成千上万个小请求把通道撑成长链表。
        let (tx, mut rx) = mpsc::channel(buffer);
        let limiter = Arc::new(Semaphore::new(max_queued_events));
        let source_name = source.name();
        let sink_name = sink.name();
        let stats = Arc::new(Stats::default());

        // 落库单独起一个任务，攒下一批和写上一批就能重叠起来。通道深度 1 =
        // 双缓冲：一批在写、一批在攒，再多就在 send 处等着，形成对 source 的背压。
        let (write_tx, write_rx) = mpsc::channel::<WriteBatch>(1);
        let mut writer: JoinHandle<Result<()>> = tokio::spawn(write_batches(
            sink,
            write_rx,
            retry,
            on_error,
            Arc::clone(&stats),
        ));

        // 内部信号：外部 Ctrl-C / stop() 会转发到这里，pipeline 自己出错时也用它
        // 叫停 source，否则 source 会在没人接收的情况下空转。
        let (stop_source, source_shutdown) = shutdown::channel();
        tokio::spawn({
            let stop_source = stop_source.clone();
            async move {
                shutdown.cancelled().await;
                stop_source.trigger();
            }
        });

        let mut source_task: JoinHandle<Result<()>> = tokio::spawn(source.run(
            SourceSender::new(tx, Arc::clone(&limiter), max_queued_events),
            source_shutdown,
        ));

        let mut pending = Pending::default();
        let mut deadline: Option<Instant> = None;
        // 写入任务提前结束时它的返回值，避免收尾时二次 await 同一个 JoinHandle。
        let mut writer_outcome: Option<Result<()>> = None;
        // 第一次 tick 不要立刻来，先攒满一个周期再报
        let mut stats_tick = stats_interval.map(|every| {
            let mut tick = tokio::time::interval_at(Instant::now() + every, every);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            tick
        });

        tracing::info!(
            source = source_name,
            sink = sink_name,
            max_queued_events,
            batch_max_events = batch.max_events,
            "pipeline 启动"
        );

        let result: Result<()> = async {
            loop {
                let timer = async {
                    match deadline {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                };
                let stats_due = async {
                    match stats_tick.as_mut() {
                        Some(tick) => {
                            tick.tick().await;
                        }
                        None => std::future::pending().await,
                    }
                };

                tokio::select! {
                    incoming = rx.recv() => match incoming {
                        Some(mut incoming) => {
                            stats.received_batches.fetch_add(1, Ordering::Relaxed);
                            stats
                                .received_events
                                .fetch_add(incoming.events.len() as u64, Ordering::Relaxed);
                            for event in incoming.events.drain(..) {
                                if let Some(event) = apply(&mut transforms, event) {
                                    pending.push(event);
                                }
                            }
                            if let Some(ack) = incoming.ack.take() {
                                pending.acks.push(ack);
                            }
                            // 这批已经进了 pending，队列名额还回去（pending 自己有
                            // batch.max_events 兜着）
                            drop(incoming);

                            if deadline.is_none() {
                                deadline = Some(Instant::now() + batch.timeout);
                            }
                            if pending.is_full(&batch) {
                                hand_off(&write_tx, &mut pending).await?;
                                deadline = None;
                            }
                        }
                        // source 结束（退出信号 / 出错），冲刷剩余数据。
                        None => {
                            hand_off(&write_tx, &mut pending).await?;
                            break;
                        }
                    },
                    _ = timer => {
                        hand_off(&write_tx, &mut pending).await?;
                        deadline = None;
                    }
                    _ = stats_due => {
                        stats.report(
                            max_queued_events - limiter.available_permits(),
                            rx.len(),
                            pending.events.len(),
                        );
                    }
                    // 写入任务只会因为「重试耗尽且 on_error = stop」提前结束。不盯着它的话，
                    // 恰好没有新数据进来时这里会一直等下去，错误要拖到下一批才暴露。
                    finished = &mut writer => {
                        writer_outcome = Some(flatten(finished));
                        break;
                    }
                }
            }
            Ok(())
        }
        .await;

        // 先让写入任务把手上的批次写完、回执发出去 —— 等回执的客户端还在等着，
        // 所以这一步必须排在 join source 之前。
        drop(write_tx);
        let writer_result = match writer_outcome {
            Some(outcome) => outcome,
            None => flatten(writer.await),
        };

        // 通知 source 收工，并关掉接收端：它下一次发送会立刻失败，不至于卡在背压上。
        // 信号量也要关：卡在等名额的 send 才会醒过来。
        stop_source.trigger();
        limiter.close();
        rx.close();
        drop(rx);
        // 释放还没回执的 ack：对应批次没能落库，等回执的客户端会收到失败。
        // 必须在等 source 退出之前丢掉，否则 source 会一直等这些回执。
        drop(pending);

        // 主循环拿到的多半只是「写入任务已退出」这种转述，写入任务自己的错误才是根因。
        let result = match (result, writer_result) {
            (_, Err(err)) | (Err(err), Ok(())) => Err(err),
            (Ok(()), Ok(())) => Ok(()),
        };

        match result {
            Ok(()) => flatten(source_task.await),
            Err(err) => {
                let _ = (&mut source_task).await;
                Err(err)
            }
        }
    }
}

/// 后台运行中的 pipeline。
pub struct RunningPipeline {
    handle: ShutdownHandle,
    task: JoinHandle<Result<()>>,
}

impl RunningPipeline {
    /// 触发优雅退出并等待收尾（剩余数据会先写完）。
    pub async fn stop(self) -> Result<()> {
        self.handle.trigger();
        flatten(self.task.await)
    }

    /// 等它自己跑完（比如 stdin 关闭、写入失败停机）。
    pub async fn wait(self) -> Result<()> {
        flatten(self.task.await)
    }

    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.handle.clone()
    }
}

#[derive(Default)]
struct Pending {
    events: Vec<MetricEvent>,
    acks: Vec<oneshot::Sender<()>>,
    bytes: usize,
}

impl Pending {
    fn push(&mut self, event: MetricEvent) {
        self.bytes += event.estimated_size();
        self.events.push(event);
    }

    fn is_full(&self, batch: &BatchConfig) -> bool {
        self.events.len() >= batch.max_events || self.bytes >= batch.max_bytes
    }

    fn take(&mut self) -> WriteBatch {
        self.bytes = 0;
        WriteBatch {
            events: std::mem::take(&mut self.events),
            acks: std::mem::take(&mut self.acks),
        }
    }
}

/// 交给写入任务的一批数据。
struct WriteBatch {
    events: Vec<MetricEvent>,
    /// 这批数据落库之后要回执的通道。
    acks: Vec<oneshot::Sender<()>>,
}

/// 顺序执行 transform，任一环节返回 `None` 就丢弃这条数据点。
fn apply(transforms: &mut [Transform], event: MetricEvent) -> Option<MetricEvent> {
    let mut current = event;
    for transform in transforms.iter_mut() {
        current = transform(current)?;
    }
    Some(current)
}

/// 把攒好的一批交给写入任务。通道满（上一批还在写）时在这里等，也就是背压点。
async fn hand_off(tx: &mpsc::Sender<WriteBatch>, pending: &mut Pending) -> Result<()> {
    if pending.events.is_empty() && pending.acks.is_empty() {
        return Ok(());
    }
    tx.send(pending.take())
        .await
        .map_err(|_| Error::other("写入任务已退出"))
}

/// 独占 sink 的写入任务：按顺序把每一批写进存储，成功后回执。
///
/// 单独成一个任务，是为了让「攒下一批」和「写上一批」重叠起来：一次 ClickHouse 往返
/// （失败时还要叠加最长 30s 退避）期间整条链路照常收数据。
///
/// 任务内部**保持串行**，回执必须按批次顺序放出去，别改成并发写。
async fn write_batches(
    mut sink: Box<dyn Sink>,
    mut batches: mpsc::Receiver<WriteBatch>,
    retry: RetryConfig,
    on_error: OnError,
    stats: Arc<Stats>,
) -> Result<()> {
    while let Some(batch) = batches.recv().await {
        // 没有数据但攒了 ack（整批都被 transform 丢掉了），照样回执。这里也要排队，
        // 不能在主循环里就地回 —— 否则会越过还在写的上一批。
        if batch.events.is_empty() {
            ack_all(batch.acks);
            continue;
        }

        let mut attempt = 1;
        let outcome = loop {
            let started = Instant::now();
            let result = sink.write(&batch.events).await;
            stats.record_write(started.elapsed(), result.is_ok(), batch.events.len());
            match result {
                Ok(()) => break Ok(()),
                Err(err) if attempt < retry.max_attempts => {
                    let backoff = retry.backoff(attempt);
                    tracing::warn!(
                        sink = sink.name(),
                        attempt,
                        ?backoff,
                        %err,
                        "写入失败，稍后重试"
                    );
                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                }
                Err(err) => break Err(err),
            }
        };

        match outcome {
            Ok(()) => {
                tracing::debug!(count = batch.events.len(), "落库成功");
                ack_all(batch.acks);
            }
            Err(err) => {
                let count = batch.events.len();
                match on_error {
                    OnError::Stop => {
                        tracing::error!(count, %err, "重试耗尽，停止 pipeline");
                        return Err(err);
                    }
                    OnError::Drop => {
                        tracing::error!(count, %err, "重试耗尽，丢弃这批数据");
                        // 不回 ack：等回执的客户端会收到失败并重发。
                    }
                }
            }
        }
    }
    Ok(())
}

fn ack_all(acks: Vec<oneshot::Sender<()>>) {
    for ack in acks {
        let _ = ack.send(());
    }
}

/// 一个统计周期内的计数。主循环和写入任务各记各的，到点由主循环一次读出并清零。
///
/// 只有这一处能看到 pipeline 自己跑得怎么样：批攒到多大、写一次多久、队列里压了多少。
/// 线上说「慢」的时候先看这条日志，比猜强。
#[derive(Default)]
struct Stats {
    received_batches: AtomicU64,
    received_events: AtomicU64,
    written_batches: AtomicU64,
    written_events: AtomicU64,
    write_failures: AtomicU64,
    write_nanos_total: AtomicU64,
    write_nanos_max: AtomicU64,
}

impl Stats {
    fn record_write(&self, took: Duration, ok: bool, events: usize) {
        let nanos = took.as_nanos().min(u64::MAX as u128) as u64;
        self.write_nanos_total.fetch_add(nanos, Ordering::Relaxed);
        self.write_nanos_max.fetch_max(nanos, Ordering::Relaxed);
        if ok {
            self.written_batches.fetch_add(1, Ordering::Relaxed);
            self.written_events
                .fetch_add(events as u64, Ordering::Relaxed);
        } else {
            self.write_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 打一条 info 并清零。`queued_events` / `queued_batches` 是队列里还没进缓冲区的，
    /// `pending_events` 是正在攒的那一批。
    fn report(&self, queued_events: usize, queued_batches: usize, pending_events: usize) {
        let received_batches = self.received_batches.swap(0, Ordering::Relaxed);
        let received_events = self.received_events.swap(0, Ordering::Relaxed);
        let written_batches = self.written_batches.swap(0, Ordering::Relaxed);
        let written_events = self.written_events.swap(0, Ordering::Relaxed);
        let write_failures = self.write_failures.swap(0, Ordering::Relaxed);
        let total = self.write_nanos_total.swap(0, Ordering::Relaxed);
        let max = self.write_nanos_max.swap(0, Ordering::Relaxed);
        let writes = written_batches + write_failures;
        let avg_ms = if writes == 0 {
            0
        } else {
            total / writes / 1_000_000
        };
        tracing::info!(
            received_batches,
            received_events,
            written_batches,
            written_events,
            write_failures,
            write_avg_ms = avg_ms,
            write_max_ms = max / 1_000_000,
            queued_events,
            queued_batches,
            pending_events,
            "pipeline 统计"
        );
    }
}

/// 等一个「该收工了」的信号。
///
/// **必须同时接 SIGTERM**：k8s 终止 Pod 发的是它，不是 SIGINT。而且容器里 metricpipe
/// 就是 PID 1（Dockerfile 的 ENTRYPOINT 是 exec 形式），内核对 PID 1 不套用默认信号
/// 动作 —— 没装 handler 的 SIGTERM 会被直接忽略，k8s 只能干等满
/// `terminationGracePeriodSeconds` 再 SIGKILL，最后一批数据就没了。
#[cfg(unix)]
async fn terminate_signal() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};

    let mut term = match signal(SignalKind::terminate()) {
        Ok(term) => term,
        // 注册不上不至于让整个进程起不来，退回到只认 Ctrl-C。
        Err(err) => {
            tracing::warn!(%err, "注册 SIGTERM 处理失败，只能靠 Ctrl-C 退出");
            let _ = tokio::signal::ctrl_c().await;
            return "SIGINT";
        }
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => "SIGINT",
        _ = term.recv() => "SIGTERM",
    }
}

#[cfg(not(unix))]
async fn terminate_signal() -> &'static str {
    let _ = tokio::signal::ctrl_c().await;
    "SIGINT"
}

fn flatten(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    match result {
        Ok(inner) => inner,
        Err(err) => Err(Error::other(format!("任务异常退出: {err}"))),
    }
}

#[derive(Default)]
pub struct PipelineBuilder {
    source: Option<Box<dyn Source>>,
    sink: Option<Box<dyn Sink>>,
    transforms: Vec<Transform>,
    batch: Option<BatchConfig>,
    retry: Option<RetryConfig>,
    buffer: Option<usize>,
    max_queued_events: Option<usize>,
    stats_interval: Option<Option<Duration>>,
    require_healthy: bool,
    on_error: Option<OnError>,
}

impl PipelineBuilder {
    pub fn source(mut self, source: impl Source) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// 同 [`Self::source`]，接一个已经装箱的 source（配置文件按 type 分派时用）。
    pub fn boxed_source(mut self, source: Box<dyn Source>) -> Self {
        self.source = Some(source);
        self
    }

    pub fn sink(mut self, sink: impl Sink) -> Self {
        self.sink = Some(Box::new(sink));
        self
    }

    /// 逐条改写/过滤数据点，可以叠加多个，按添加顺序执行。
    pub fn transform<F>(mut self, transform: F) -> Self
    where
        F: FnMut(MetricEvent) -> Option<MetricEvent> + Send + 'static,
    {
        self.transforms.push(Box::new(transform));
        self
    }

    pub fn batch(mut self, batch: BatchConfig) -> Self {
        self.batch = Some(batch);
        self
    }

    pub fn retry(mut self, retry: RetryConfig) -> Self {
        self.retry = Some(retry);
        self
    }

    /// source 与 sink 之间的队列深度（按批计）。队列满了 source 会被自然阻塞。
    pub fn buffer(mut self, buffer: usize) -> Self {
        self.buffer = Some(buffer.max(1));
        self
    }

    /// 队列里最多压多少个数据点（默认 [`DEFAULT_MAX_QUEUED_EVENTS`]）。
    ///
    /// 这才是内存上界：一次导出请求动辄上万个点，`buffer` 按批数限不住。到了上限
    /// source 会等（OTLP 那边等满 `enqueue_timeout` 就回「稍后重试」）。
    pub fn max_queued_events(mut self, events: usize) -> Self {
        self.max_queued_events = Some(events.max(1));
        self
    }

    /// 多久打一条运行统计（收了多少、写了多少、写一次多久、队列压了多少）。
    /// 默认 [`DEFAULT_STATS_INTERVAL`]；`None` 关掉。
    pub fn stats_interval(mut self, every: Option<Duration>) -> Self {
        self.stats_interval = Some(every.filter(|d| !d.is_zero()));
        self
    }

    /// healthcheck 失败就不启动。
    pub fn require_healthy(mut self, require: bool) -> Self {
        self.require_healthy = require;
        self
    }

    pub fn on_error(mut self, on_error: OnError) -> Self {
        self.on_error = Some(on_error);
        self
    }

    pub fn build(self) -> Result<Pipeline> {
        Ok(Pipeline {
            source: self.source.ok_or_else(|| Error::config("缺少 source"))?,
            sink: self.sink.ok_or_else(|| Error::config("缺少 sink"))?,
            transforms: self.transforms,
            batch: self.batch.unwrap_or_default(),
            retry: self.retry.unwrap_or_default(),
            buffer: self.buffer.unwrap_or(64),
            max_queued_events: self.max_queued_events.unwrap_or(DEFAULT_MAX_QUEUED_EVENTS),
            stats_interval: self.stats_interval.unwrap_or(Some(DEFAULT_STATS_INTERVAL)),
            require_healthy: self.require_healthy,
            on_error: self.on_error.unwrap_or(OnError::Stop),
        })
    }
}
