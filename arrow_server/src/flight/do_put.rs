use std::fs::File;

use arrow::{array::RecordBatch, ipc::writer::FileWriter};
use arrow_flight::PutResult;
use parquet::arrow::ArrowWriter;
use tonic::Status;

pub async fn write_batch(batchs: Vec<RecordBatch>) -> Vec<Result<PutResult, Status>> {
    // 打开一个文件用于写入 Parquet
    todo!()
}
