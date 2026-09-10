use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("配置错误: {0}")]
    Config(String),

    #[error("接收失败: {0}")]
    Source(#[source] BoxError),

    #[error("入库失败: {0}")]
    Sink(#[source] BoxError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// 带上下文的 IO 错误：光看 "Address already in use" 不知道是哪个端口。
    #[error("{0}: {1}")]
    IoContext(String, #[source] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error("{0}")]
    Other(String),
}

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

impl Error {
    pub fn config(msg: impl fmt::Display) -> Self {
        Error::Config(msg.to_string())
    }

    pub fn source(err: impl Into<BoxError>) -> Self {
        Error::Source(err.into())
    }

    pub fn sink(err: impl Into<BoxError>) -> Self {
        Error::Sink(err.into())
    }

    pub fn other(msg: impl fmt::Display) -> Self {
        Error::Other(msg.to_string())
    }

    /// 给 IO 错误补上「在做什么」，权限问题再附一句怎么办。
    pub fn io(context: impl fmt::Display, err: std::io::Error) -> Self {
        let context = match err.kind() {
            std::io::ErrorKind::PermissionDenied => {
                format!("{context}（当前用户没有权限；1024 以下的端口需要 root 或 CAP_NET_BIND_SERVICE）")
            }
            std::io::ErrorKind::AddrInUse => {
                format!("{context}（端口被占用；是不是已经有一个实例在跑）")
            }
            _ => context.to_string(),
        };
        Error::IoContext(context, err)
    }
}
