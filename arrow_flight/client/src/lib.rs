use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    json::ReaderBuilder,
};
use arrow_flight::{utils::batches_to_flight_data, FlightClient, FlightData};

use anyhow::Result;
use serde::Serialize;
use tokio_stream::StreamExt;

pub async fn do_put_test(cleint: &mut FlightClient) {
    let batch = create_batch();
    match batch {
        Ok(b) => {
            // 创建一个流，用于发送数据到服务端
            let input_stream = futures::stream::iter(b).map(Ok);
            let a = cleint.do_put(input_stream).await.unwrap();
        }
        Err(_) => todo!(),
    }
}

#[derive(Serialize)]
struct MyStruct {
    int32: i32,
    string: String,
}
pub fn create_batch() -> Result<Vec<FlightData>, ArrowError> {
    let schema = Schema::new(vec![
        Field::new("int32", DataType::Int32, false),
        Field::new("string", DataType::Utf8, false),
    ]);

    let mut vecs = Vec::new();
    for _ in 0..5 {
        let rows = vec![
            MyStruct {
                int32: 5,
                string: "bar".to_string(),
            },
            MyStruct {
                int32: 8,
                string: "foo".to_string(),
            },
        ];

        let mut decoder = ReaderBuilder::new(Arc::new(schema.clone()))
            .build_decoder()
            .unwrap();
        decoder.serialize(&rows).unwrap();

        let batch: arrow::array::RecordBatch = decoder.flush().unwrap().unwrap();
        vecs.push(batch);
    }
    println!("vecs = {:?}", vecs.len());
    batches_to_flight_data(&schema, vecs)
}
