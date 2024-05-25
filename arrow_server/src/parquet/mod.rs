mod active_file;
use std::sync::Arc;

use self::active_file::ActiveFile;
use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::*};
use dashmap::DashMap;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::info;

const PATH: &'static str = "";

/**
 * 像文件追加内容
 */
#[allow(async_fn_in_trait)]
pub trait Append {
    type Resp;
    async fn append(&mut self, buf: &RecordBatch) -> Self::Resp;
}

#[derive(Debug, Clone)]
pub struct ParquetConfig {
    pub path: &'static str,
    pub max_file_size: usize,
}

/**
 * 初始化parquetService
 */
pub fn parquet_service(conf: ParquetConfig) -> (ParquetService, Sender<RecordBatch>) {
    let (rt, rx) = mpsc::channel(128);
    (
        ParquetService {
            revc: rx,
            conf,
            active_file: None,
            old_file: DashMap::new(),
        },
        rt,
    )
}

#[derive(Debug)]
pub struct ParquetService {
    // 数据接收端
    revc: Receiver<RecordBatch>,
    // 配置
    conf: ParquetConfig,
    // 当前写入的文件
    active_file: Option<ActiveFile>,
    // 已经持久化的文件
    old_file: DashMap<String, String>,
}

impl ParquetService {
    async fn init_active_file(&mut self, schema: Schema) {
        info!("init active file");
        let active = ActiveFile::init(self.conf.max_file_size, self.conf.path, schema)
            .await
            .unwrap();
        self.active_file = Some(active);
    }

    async fn active_file_is_none(&self) -> bool {
        true
    }

    async fn update_active_file(mut self) {
        self.active_file.unwrap().close_writer().await;
        self.active_file = Some(
            ActiveFile::init(self.conf.max_file_size, self.conf.path, create_schema())
                .await
                .unwrap(),
        );
    }
}

fn create_schema() -> Schema {
    let schema = Schema::new(vec![
        Field::new("name", DataType::Int32, false),
        Field::new("age", DataType::Int32, false),
        Field::new("address", DataType::Int32, false),
        Field::new("time", DataType::UInt64, true),
    ]);
    schema
}

impl ParquetService {
    pub async fn start(mut self) {
        self.init_active_file(create_schema()).await;
        let mut n = 1;
        loop {
            match self.revc.recv().await {
                Some(batch) => {
                    let _ = self.append(&batch).await;
                }
                None => {
                    info!("there is no msg are received !");
                }
            }
            n += 1;
            info!("n = {}", n);
            if n == 9999 {
                break;
            }
        }
        self.update_active_file().await;
    }
}

impl Append for ParquetService {
    type Resp = Result<()>;

    async fn append(&mut self, buf: &RecordBatch) -> Self::Resp {
        info!("ParquetService Append");
        self.active_file.as_mut().unwrap().append(buf).await
    }
}
