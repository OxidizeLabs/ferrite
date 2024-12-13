use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::recovery::log_manager::LogManager;
use std::sync::Arc;

pub struct CheckpointManager {
    log_manager: Arc<LogManager>,
    transaction_manager: Arc<TransactionManager>,
    buffer_pool_manager: Arc<BufferPoolManager>,
}

impl CheckpointManager {
    pub fn new(
        p0: Arc<TransactionManager>,
        p1: Arc<LogManager>,
        p2: Arc<BufferPoolManager>,
    ) -> Self {
        todo!()
    }
}
