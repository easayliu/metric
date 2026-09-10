//! OTLP 接收端：gRPC（默认 4317）和 HTTP（默认 4318，`POST /v1/metrics`）两个入口，
//! 也就是 OTel SDK / Java agent 默认往外发的那两个地址。
//!
//! 两个入口收到的请求都经 [`crate::otlp::convert`] 拆成 [`MetricEvent`] 交给 pipeline。
//! 队列满了不会无限等：超过 `enqueue_timeout` 就回 `UNAVAILABLE` / `503`，SDK 会按
//! 自己的退避重发 —— 这正是 OTLP 规定的「可重试」应答，比把数据堆在内存里稳。

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tonic::codec::CompressionEncoding;
use tonic::transport::server::TcpIncoming;

use crate::error::{Error, Result};
use crate::event::MetricEvent;
use crate::otlp;
use crate::shutdown::{self, Shutdown};
use crate::source::{Source, SourceSender};

pub const DEFAULT_GRPC_ADDR: &str = "0.0.0.0:4317";
pub const DEFAULT_HTTP_ADDR: &str = "0.0.0.0:4318";
/// 单个请求（解压后）的大小上限。OTel collector 的默认是 4MiB，SDK 一个上报周期的
/// 指标远到不了，放宽一些给指标特别多（比如整机 host metrics）的来源留余量。
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(5);

/// 监听在哪：给地址由 source 自己绑，或者把已经绑好的 listener 交进来（测试里用
/// 端口 0 时要先知道实际端口）。
enum Endpoint {
    Addr(SocketAddr),
    Bound(std::net::TcpListener),
}

impl Endpoint {
    async fn listen(self, what: &str) -> Result<TcpListener> {
        match self {
            Endpoint::Addr(addr) => TcpListener::bind(addr)
                .await
                .map_err(|err| Error::io(format!("监听 {what} {addr} 失败"), err)),
            Endpoint::Bound(listener) => {
                listener.set_nonblocking(true)?;
                Ok(TcpListener::from_std(listener)?)
            }
        }
    }
}

pub struct OtlpSource {
    grpc: Option<Endpoint>,
    http: Option<Endpoint>,
    max_request_bytes: usize,
    enqueue_timeout: Duration,
    wait_for_write: bool,
    fields: Option<Arc<BTreeMap<String, Value>>>,
}

impl OtlpSource {
    /// gRPC 监听 `0.0.0.0:4317`、HTTP 监听 `0.0.0.0:4318`。
    pub fn new() -> Self {
        Self {
            grpc: Some(Endpoint::Addr(DEFAULT_GRPC_ADDR.parse().expect("常量地址"))),
            http: Some(Endpoint::Addr(DEFAULT_HTTP_ADDR.parse().expect("常量地址"))),
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            enqueue_timeout: DEFAULT_ENQUEUE_TIMEOUT,
            wait_for_write: false,
            fields: None,
        }
    }

    pub fn grpc(mut self, addr: SocketAddr) -> Self {
        self.grpc = Some(Endpoint::Addr(addr));
        self
    }

    /// 用已经绑好的 listener 收 gRPC。
    pub fn grpc_listener(mut self, listener: std::net::TcpListener) -> Self {
        self.grpc = Some(Endpoint::Bound(listener));
        self
    }

    pub fn no_grpc(mut self) -> Self {
        self.grpc = None;
        self
    }

    pub fn http(mut self, addr: SocketAddr) -> Self {
        self.http = Some(Endpoint::Addr(addr));
        self
    }

    /// 用已经绑好的 listener 收 HTTP。
    pub fn http_listener(mut self, listener: std::net::TcpListener) -> Self {
        self.http = Some(Endpoint::Bound(listener));
        self
    }

    pub fn no_http(mut self) -> Self {
        self.http = None;
        self
    }

    /// 单个请求（解压后）的大小上限，超过直接拒收。
    pub fn max_request_bytes(mut self, limit: usize) -> Self {
        self.max_request_bytes = limit.max(1024);
        self
    }

    /// 下游队列满时最多等多久，超时回「稍后重试」。
    pub fn enqueue_timeout(mut self, timeout: Duration) -> Self {
        self.enqueue_timeout = timeout;
        self
    }

    /// 等这批数据**真正写进存储**再给客户端回成功（默认关）。
    ///
    /// 打开后落库失败会直接反映成客户端的导出失败，SDK 会重发，等于不用磁盘缓冲
    /// 也有「至少一次」；代价是每个请求要多等一个攒批周期加一次写入的时间。
    pub fn wait_for_write(mut self, enabled: bool) -> Self {
        self.wait_for_write = enabled;
        self
    }

    /// 附加到每条数据点 上的静态字段。
    pub fn fields(mut self, fields: BTreeMap<String, Value>) -> Self {
        self.fields = (!fields.is_empty()).then(|| Arc::new(fields));
        self
    }
}

impl Default for OtlpSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for OtlpSource {
    async fn run(self: Box<Self>, out: SourceSender, shutdown: Shutdown) -> Result<()> {
        let Self {
            grpc,
            http,
            max_request_bytes,
            enqueue_timeout,
            wait_for_write,
            fields,
        } = *self;
        if grpc.is_none() && http.is_none() {
            return Err(Error::config("OTLP source 的 grpc 和 http 至少要开一个"));
        }

        // 先把端口都绑好再起服务：一个绑不上就整个不启动，别留一半在跑
        let grpc = match grpc {
            Some(endpoint) => Some(endpoint.listen("OTLP/gRPC").await?),
            None => None,
        };
        let http = match http {
            Some(endpoint) => Some(endpoint.listen("OTLP/HTTP").await?),
            None => None,
        };

        let receiver = Arc::new(Receiver {
            out,
            max_request_bytes,
            enqueue_timeout,
            wait_for_write,
            fields,
        });

        // 内部开关：外部退出信号转发进来；任何一个监听自己退了（出错）也用它叫停
        // 另一个，否则 gRPC 挂了 HTTP 还在收，pipeline 却永远等不到 source 结束。
        let (stop, inner) = shutdown::channel();
        tokio::spawn({
            let stop = stop.clone();
            async move {
                shutdown.cancelled().await;
                stop.trigger();
            }
        });

        let mut servers = JoinSet::new();
        if let Some(listener) = grpc {
            tracing::info!(addr = %listener.local_addr()?, "OTLP/gRPC 监听中");
            servers.spawn(serve_grpc(
                listener,
                Arc::clone(&receiver),
                inner.clone(),
                max_request_bytes,
            ));
        }
        if let Some(listener) = http {
            tracing::info!(addr = %listener.local_addr()?, "OTLP/HTTP 监听中");
            servers.spawn(serve_http(
                listener,
                Arc::clone(&receiver),
                inner.clone(),
                max_request_bytes,
            ));
        }

        let mut result = Ok(());
        while let Some(finished) = servers.join_next().await {
            let outcome = match finished {
                Ok(outcome) => outcome,
                Err(err) => Err(Error::other(format!("监听任务异常退出: {err}"))),
            };
            if let Err(err) = outcome {
                if result.is_ok() {
                    result = Err(err);
                }
            }
            stop.trigger();
        }
        result
    }

    fn name(&self) -> &'static str {
        "otlp"
    }
}

/// 两个入口共用的处理逻辑：转换、入队、等回执。
struct Receiver {
    out: SourceSender,
    max_request_bytes: usize,
    enqueue_timeout: Duration,
    wait_for_write: bool,
    fields: Option<Arc<BTreeMap<String, Value>>>,
}

/// 为什么没收：都是「稍后重试」一类，客户端看到的是 UNAVAILABLE / 503。
#[derive(Debug, Clone, Copy)]
enum Reject {
    /// 下游队列满，等了 `enqueue_timeout` 还没进去。
    Busy,
    /// pipeline 已经关了（正在退出）。
    Closed,
    /// `wait_for_write` 打开、这批数据没能写进存储。
    WriteFailed,
}

impl Reject {
    fn message(self) -> &'static str {
        match self {
            Reject::Busy => "下游队列已满，稍后重试",
            Reject::Closed => "正在退出，稍后重试",
            Reject::WriteFailed => "写入存储失败，稍后重试",
        }
    }
}

impl Receiver {
    async fn accept(
        &self,
        request: ExportMetricsServiceRequest,
    ) -> std::result::Result<usize, Reject> {
        let events: Vec<MetricEvent> = otlp::convert(request, self.fields.as_ref());
        let count = events.len();
        if count == 0 {
            return Ok(0);
        }

        if self.wait_for_write {
            let ack = tokio::time::timeout(self.enqueue_timeout, self.out.send_with_ack(events))
                .await
                .map_err(|_| Reject::Busy)?
                .map_err(|_| Reject::Closed)?;
            // 发送端被丢弃 = 这批没落库（重试耗尽 / 退出时没写完）
            ack.await.map_err(|_| Reject::WriteFailed)?;
        } else {
            tokio::time::timeout(self.enqueue_timeout, self.out.send(events))
                .await
                .map_err(|_| Reject::Busy)?
                .map_err(|_| Reject::Closed)?;
        }
        Ok(count)
    }
}

struct Grpc(Arc<Receiver>);

#[tonic::async_trait]
impl MetricsService for Grpc {
    async fn export(
        &self,
        request: tonic::Request<ExportMetricsServiceRequest>,
    ) -> std::result::Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
        match self.0.accept(request.into_inner()).await {
            Ok(count) => {
                tracing::debug!(count, transport = "grpc", "收到数据点");
                Ok(tonic::Response::new(ExportMetricsServiceResponse::default()))
            }
            Err(reject) => {
                tracing::warn!(transport = "grpc", reason = ?reject, "拒收一批数据点");
                // OTLP 规定 UNAVAILABLE 是可重试的，SDK 会退避重发
                Err(tonic::Status::unavailable(reject.message()))
            }
        }
    }
}

async fn serve_grpc(
    listener: TcpListener,
    receiver: Arc<Receiver>,
    shutdown: Shutdown,
    max_request_bytes: usize,
) -> Result<()> {
    let service = MetricsServiceServer::new(Grpc(receiver))
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(max_request_bytes);

    tonic::transport::Server::builder()
        .add_service(service)
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), async move {
            shutdown.cancelled().await
        })
        .await
        .map_err(|err| Error::source(format!("OTLP/gRPC 服务退出: {err}")))
}

async fn serve_http(
    listener: TcpListener,
    receiver: Arc<Receiver>,
    shutdown: Shutdown,
    max_request_bytes: usize,
) -> Result<()> {
    let app = Router::new()
        .route("/v1/metrics", post(export_http))
        // 压缩前的 body 也按同一个上限卡，解压后再卡一次（见 decode_body）
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .with_state(receiver);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .map_err(|err| Error::source(format!("OTLP/HTTP 服务退出: {err}")))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Protobuf,
    Json,
}

impl Encoding {
    fn content_type(self) -> &'static str {
        match self {
            Encoding::Protobuf => "application/x-protobuf",
            Encoding::Json => "application/json",
        }
    }
}

/// `POST /v1/metrics`：按 Content-Type 选 protobuf / JSON 解码，Content-Encoding: gzip
/// 先解压。应答按 OTLP/HTTP 的规定用和请求相同的编码。
async fn export_http(
    State(receiver): State<Arc<Receiver>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let encoding = match headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
        })
        .as_deref()
    {
        Some("application/x-protobuf") => Encoding::Protobuf,
        Some("application/json") => Encoding::Json,
        _ => {
            return (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Content-Type 只支持 application/x-protobuf 或 application/json",
            )
                .into_response()
        }
    };

    let body = match decode_body(&headers, body, receiver.max_request_bytes) {
        Ok(body) => body,
        Err(reply) => return reply.into_response(),
    };

    let request = match encoding {
        Encoding::Protobuf => ExportMetricsServiceRequest::decode(body.as_ref())
            .map_err(|err| format!("protobuf 解析失败: {err}")),
        // 走 otlp::decode_json_bytes 而不是 serde_json：整数值的数据点要先修一道，
        // 见那里的说明
        Encoding::Json => {
            otlp::decode_json_bytes(&body).map_err(|err| format!("JSON 解析失败: {err}"))
        }
    };
    let request = match request {
        Ok(request) => request,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };

    match receiver.accept(request).await {
        Ok(count) => {
            tracing::debug!(count, transport = "http", "收到数据点");
            let body = match encoding {
                Encoding::Protobuf => ExportMetricsServiceResponse::default().encode_to_vec(),
                Encoding::Json => b"{}".to_vec(),
            };
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, encoding.content_type())],
                body,
            )
                .into_response()
        }
        Err(reject) => {
            tracing::warn!(transport = "http", reason = ?reject, "拒收一批数据点");
            // 503 + Retry-After 是 OTLP/HTTP 规定的「可重试」应答
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::RETRY_AFTER, "1")],
                reject.message(),
            )
                .into_response()
        }
    }
}

/// 按 Content-Encoding 解压。只认 gzip（SDK 只会发这个）；解压后仍然按上限卡，
/// 免得一个几 KB 的压缩包撑出几百 MB。
fn decode_body(
    headers: &HeaderMap,
    body: Bytes,
    limit: usize,
) -> std::result::Result<Bytes, (StatusCode, String)> {
    match headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_ascii_lowercase())
        .as_deref()
    {
        None | Some("") | Some("identity") => Ok(body),
        Some("gzip") => {
            use std::io::Read;
            let mut out = Vec::with_capacity(body.len() * 4);
            flate2::read::GzDecoder::new(body.as_ref())
                .take(limit as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|err| (StatusCode::BAD_REQUEST, format!("gzip 解压失败: {err}")))?;
            if out.len() > limit {
                return Err((
                    StatusCode::PAYLOAD_TOO_LARGE,
                    format!("解压后超过 {limit} 字节上限"),
                ));
            }
            Ok(Bytes::from(out))
        }
        Some(other) => Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("不支持的 Content-Encoding: {other}，只认 gzip"),
        )),
    }
}
