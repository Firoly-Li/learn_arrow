use std::{fmt::Debug, sync::Arc};

use crate::{error::ArrowError, now};
use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::Schema};
use parquet::{arrow::AsyncArrowWriter, basic::Compression, file::properties::WriterProperties};
use tokio::fs::{File, OpenOptions};
use tracing::info;

use super::Append;

pub struct ActiveFile {
    file_name: String,
    // 数据写入
    writer: AsyncArrowWriter<File>,
    // 规定的最大文件大小
    max_size: usize,
    // 文件路径
    file_path: String,
}

impl Debug for ActiveFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveFile")
            .field("file_name", &self.file_name)
            .field("max_size", &self.max_size)
            .finish()
    }
}

impl ActiveFile {
    pub async fn init(max_size: usize, path: &str, schema: Schema) -> Result<Self> {
        let times = now();
        let file_path = path.to_owned() + times.to_string().as_str() + ".tssp";
        info!("path = {:?}", file_path);

        if let Ok(_file) = File::create(file_path.clone()).await {
            info!("open active file success");
            let file = OpenOptions::new()
                .append(true)
                .read(true)
                .open(file_path)
                .await
                .unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            Ok(Self {
                file_name: times.to_string(),
                writer: AsyncArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap(),
                max_size,
                file_path: path.to_string(),
            })
        } else {
            Err(ArrowError::CreateActiveFileError.into())
        }
    }

    /**
     * 是否可写
     * TODO 未实现
     */
    pub fn write_enable(&self) -> bool {
        true
    }

    pub async fn close_writer(self) {
        let _ = self.writer.close().await;
    }
}

impl Append for ActiveFile {
    type Resp = Result<()>;
    async fn append(&mut self, buf: &RecordBatch) -> Self::Resp {
        info!("ActiveFile Append");
        if self.writer.in_progress_size() > 1000 {
            let _ = self.writer.flush().await;
        }
        let _ = self.writer.write(buf).await;
        Ok(())
    }
}

/**
 * 已经持久化的文件
 */
#[derive(Debug)]
pub struct OldFile(File);
