use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    json::ReaderBuilder,
};
use arrow_flight::{utils::batches_to_flight_data, FlightClient, FlightData, FlightDescriptor};

use anyhow::Result;
use futures::TryStreamExt;
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

/**
 * 
 */
pub async fn do_get_flight_info(cleint: &mut FlightClient) {
    let desc = FlightDescriptor::new_path(vec!["0".to_string()]);
    let resp = cleint.get_flight_info(desc).await;
    match resp {
        Ok(resp) => println!("flight_info = {:?}", resp),
        Err(_) => println!("error"),
    }
}


pub async fn do_list_flights(cleint: &mut FlightClient) {
    let resp = cleint.list_flights("desc").await;
    match resp {
        
        Ok(resp) => {
            let response: Vec<_> = resp
            .try_collect()
            .await
            .expect("Error streaming data");
            println!("flight_info = {:?}", response);
        }
        Err(_) => println!("error"),
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
