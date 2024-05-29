use anyhow::Ok;
use arrow::array::RecordBatch;
use dashmap::DashMap;


use super::{value::Values, MemEngine};


#[derive(Debug)]
pub(crate) struct MemDBCore {
    tables: DashMap<String,DashMap<String,Values>>
}

impl MemDBCore {
    fn new() -> Self {
        Self {
            tables: DashMap::new()
        }
    }
}

impl MemEngine for MemDBCore {
    async fn insert(&mut self,batch: &RecordBatch) -> anyhow::Result<bool> {
        Ok(true)
    }

    async fn insert_batch(&mut self, batchs: &Vec<RecordBatch>) -> Vec<anyhow::Result<bool>> {
        let mut results = Vec::new();
        for batch in batchs {
           results.push(self.insert(batch).await);
        }
        results
    }

    async fn get(&self,key: impl Into<String>) -> anyhow::Result<RecordBatch> {
        todo!()
    }

    async fn delete(&self,key: impl Into<String>) -> anyhow::Result<bool> {
        Ok(true)
    }
}




#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{array::{Int32Array, RecordBatch, UInt64Array}, datatypes::*};

    use crate::memdb::{core::MemDBCore, MemEngine};


    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn memdb_core_should_be_work() {
        let mut db = MemDBCore::new();
        let schema = create_schema();
        let batch = create_batch(schema.clone());
        let _ = db.insert(&batch).await;
        println!("db: {:?}", db);
    }


    fn create_schema() -> Arc<Schema> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Int32, false),
            Field::new("age", DataType::Int32, false),
            Field::new("address", DataType::Int32, false),
            Field::new("time", DataType::UInt64, true),
        ]));
        schema
    }

    fn create_batch(schema: Arc<Schema>) -> RecordBatch {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
                Arc::new(UInt64Array::from(vec![None, None, Some(9)])),
            ],
        )
        .unwrap();
        batch
    }
}