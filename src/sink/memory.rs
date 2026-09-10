//! 写进内存，给测试和示例用。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::error::Result;
use crate::event::MetricEvent;
use crate::sink::Sink;

#[derive(Clone, Default)]
pub struct MemorySink {
    events: Arc<Mutex<Vec<MetricEvent>>>,
}

impl MemorySink {
    pub fn new() -> Self {
        Self::default()
    }

    /// 拿到共享句柄，写入的数据点都会出现在这里。
    pub fn events(&self) -> Arc<Mutex<Vec<MetricEvent>>> {
        Arc::clone(&self.events)
    }

    pub fn len(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl Sink for MemorySink {
    async fn write(&mut self, events: &[MetricEvent]) -> Result<()> {
        self.events.lock().unwrap().extend_from_slice(events);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "memory"
    }
}
