use std::{collections::HashMap, sync::Arc, time::Duration};

use arrow::{
    array::{Int32Array, RecordBatch, UInt64Array},
    datatypes::*,
};
use arrow_server::parquet::*;

use tokio::time::sleep;
use tracing::info;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_service_test() {
    tracing_subscriber::fmt::init();
    let conf = ParquetConfig {
        path: "/Users/firoly/Documents/code/rust/learn_rust/learn_arrow/datafusion/server/test/",
        max_file_size: 1024,
    };
    let (service, sender) = parquet_service(conf);

    tokio::spawn(async move {
        service.start().await;
    });
    for _ in 0..10000 {
        info!("send batch");
        let _ = sender.send(create_batch()).await;
        sleep(Duration::from_millis(2)).await;
    }
}

fn create_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Int32, false),
        Field::new("age", DataType::Int32, false),
        Field::new("address", DataType::Int32, false),
        Field::new("time", DataType::UInt64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            ])),
            Arc::new(Int32Array::from(vec![
                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            ])),
            Arc::new(Int32Array::from(vec![
                7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
            ])),
            Arc::new(UInt64Array::from(vec![
                None,
                None,
                Some(9),
                None,
                None,
                Some(10),
                None,
                None,
                Some(11),
                None,
                None,
                Some(12),
                None,
                None,
                Some(13),
            ])),
        ],
    )
    .unwrap();
    batch
}

/**
 * 场景：
 * 客户端需要查询一大批数据比如：某台设备一个月的数据
 * 步骤：
 * 1. 客户端：get_fight_info 获取到 FlightInfo
 * 2. 客户端：通过 FlightInfo 获取到 Ticket
 * 3. 客户端：通过 Ticket 获取到 Stream
 */
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn do_get_test() {
    // 1、启动server

    let batch = create_batch();
    println!("batch = {:?}", batch);
}
