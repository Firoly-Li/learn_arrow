mod core;
mod index;
mod value;

use std::fmt;

use anyhow::Result;
use arrow::array::RecordBatch;
use bytes::Bytes;
use self::core::MemDBCore;



pub struct MemDB {
    inner: MemDBCore
}

/**
 * 定义了一些内存数据库的方法
 */
#[allow(async_fn_in_trait)]
pub trait MemEngine {
    
    async fn insert(&mut self,batch: &RecordBatch) -> Result<bool>;
    
    async fn insert_batch(&mut self, batchs: &Vec<RecordBatch>) -> Vec<anyhow::Result<bool>>;

    async fn get(&self,key: impl Into<String>) -> Result<RecordBatch>;

    async fn delete(&self,key: impl Into<String>) -> Result<bool>;

}

pub trait Value {
    fn value<T: TryFrom<Bytes> + Clone + fmt::Debug>(&self) -> Result<T>;
}