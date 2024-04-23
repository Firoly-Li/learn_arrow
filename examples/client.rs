use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    json::ReaderBuilder,
};
use learn_arrow::hw::helloworld::{greeter_client::GreeterClient, HelloRequest};
use prost::bytes::Bytes;
use serde::Serialize;
use tonic::{codec::Encoder, Request};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = GreeterClient::connect("http://127.0.0.1:50051").await?;

    let req = HelloRequest {
        name: "world".to_string(),
        arrow_msg: Bytes::from("bytes").to_vec(),
    };

    let res = client.say_hello(Request::new(req)).await?;

    println!("RESPONSE: {:#?}", res);

    Ok(())
}

#[derive(Serialize)]
struct MyStruct {
    int32: i32,
    string: String,
}
fn create_batch() -> arrow::array::RecordBatch {
    let schema = Schema::new(vec![
        Field::new("int32", DataType::Int32, false),
        Field::new("string", DataType::Utf8, false),
    ]);

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

    let mut decoder = ReaderBuilder::new(Arc::new(schema))
        .build_decoder()
        .unwrap();
    decoder.serialize(&rows).unwrap();

    let batch = decoder.flush().unwrap().unwrap();

    batch
}
