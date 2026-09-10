//! 攒批与重试参数。

use std::time::Duration;

/// 一批数据在什么条件下写入 sink：三个条件任一满足即触发。
#[derive(Clone, Debug)]
pub struct BatchConfig {
    /// 最多攒多少个数据点。
    pub max_events: usize,
    /// 最多攒多少字节（按 JSON 估算）。
    pub max_bytes: usize,
    /// 距离第一条数据进入缓冲区超过这个时间就发走。
    pub timeout: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_events: 10_000,
            max_bytes: 8 * 1024 * 1024,
            timeout: Duration::from_secs(1),
        }
    }
}

impl BatchConfig {
    pub fn max_events(mut self, max_events: usize) -> Self {
        self.max_events = max_events.max(1);
        self
    }

    pub fn max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes.max(1);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// 写入失败后的重试策略：指数退避。
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// 总尝试次数（含第一次）。设为 1 表示不重试。
    pub max_attempts: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
        }
    }
}

impl RetryConfig {
    /// 第 `attempt` 次失败后应当等待多久（`attempt` 从 1 开始）。
    pub fn backoff(&self, attempt: usize) -> Duration {
        let exp = self
            .initial_backoff
            .saturating_mul(1u32 << (attempt.min(16) - 1).min(16));
        exp.min(self.max_backoff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let retry = RetryConfig {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        };
        assert_eq!(retry.backoff(1), Duration::from_millis(100));
        assert_eq!(retry.backoff(2), Duration::from_millis(200));
        assert_eq!(retry.backoff(3), Duration::from_millis(400));
        assert_eq!(retry.backoff(9), Duration::from_secs(1));
    }
}
