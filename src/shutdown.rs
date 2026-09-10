//! 优雅退出信号：一个广播开关，触发后 source 停止接收、pipeline 冲刷剩余数据。

use tokio::sync::watch;

/// 传给 source 的只读信号。可自由 clone。
#[derive(Clone, Debug)]
pub struct Shutdown(watch::Receiver<bool>);

impl Shutdown {
    /// 等待退出信号。可以直接放进 `tokio::select!`。
    pub async fn cancelled(&self) {
        let mut rx = self.0.clone();
        loop {
            if *rx.borrow_and_update() {
                return;
            }
            // 发送端被丢弃同样视为退出。
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    pub fn is_triggered(&self) -> bool {
        *self.0.borrow()
    }
}

/// 触发端，由 pipeline 的持有者掌握。
#[derive(Clone, Debug)]
pub struct ShutdownHandle(watch::Sender<bool>);

impl ShutdownHandle {
    pub fn trigger(&self) {
        let _ = self.0.send(true);
    }

    pub fn subscribe(&self) -> Shutdown {
        Shutdown(self.0.subscribe())
    }
}

pub fn channel() -> (ShutdownHandle, Shutdown) {
    let (tx, rx) = watch::channel(false);
    (ShutdownHandle(tx), Shutdown(rx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn triggers_all_subscribers() {
        let (handle, shutdown) = channel();
        let other = handle.subscribe();
        assert!(!shutdown.is_triggered());
        handle.trigger();
        shutdown.cancelled().await;
        other.cancelled().await;
        assert!(shutdown.is_triggered());
    }

    #[tokio::test]
    async fn dropped_handle_also_releases() {
        let (handle, shutdown) = channel();
        drop(handle);
        shutdown.cancelled().await;
    }
}
