use std::sync::Arc;

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema},
    json::ReaderBuilder,
};
use arrow_flight::{
    Criteria, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};

use prost::bytes::Bytes;
use serde::Serialize;
use tonic::{Request, Result, Status};

/**
 * 执行器
 */
pub trait Executor {
    type Response;

    fn execute(&self) -> Self::Response;
}

/**
 * HandshakeRequest 执行器
 */
impl Executor for HandshakeRequest {
    type Response = Result<HandshakeResponse, Status>;

    fn execute(&self) -> Self::Response {
        let handshake_response = HandshakeResponse {
            protocol_version: 1,
            payload: Bytes::from("hello"),
        };
        Ok(handshake_response)
    }
}

/**
 * 请求执行器
 */
impl Executor for Criteria {
    // type Response = Result<Vec<FlightInfo>, Status>;
    type Response = Vec<Result<FlightInfo, Status>>;
    fn execute(&self) -> Self::Response {
        let infos = vec![
            Ok(test_flight_info(&FlightDescriptor::new_cmd("foo"))),
            Ok(test_flight_info(&FlightDescriptor::new_cmd("bar"))),
        ];
        infos
    }
}

fn test_flight_info(request: &FlightDescriptor) -> FlightInfo {
    FlightInfo {
        schema: Bytes::new(),
        endpoint: vec![],
        flight_descriptor: Some(request.clone()),
        total_bytes: 123,
        total_records: 456,
        ordered: false,
        app_metadata: Bytes::new(),
    }
}

impl Executor for Ticket {
    type Response = Vec<Result<RecordBatch, Status>>;

    fn execute(&self) -> Self::Response {
        let mut vecs = Vec::new();
        vecs.push(create_batch(6));
        vecs.push(create_batch(9));
        vecs
    }
}

#[derive(Serialize)]
struct MyStruct {
    int32: i32,
    string: String,
}

fn create_batch(n: i32) -> Result<RecordBatch, Status> {
    let schema = Schema::new(vec![
        Field::new("int32", DataType::Int32, false),
        Field::new("string", DataType::Utf8, false),
    ]);

    let rows = vec![
        MyStruct {
            int32: n,
            string: "bar".to_string(),
        },
        MyStruct {
            int32: n + 1,
            string: "foo".to_string(),
        },
    ];

    let mut decoder = ReaderBuilder::new(Arc::new(schema.clone()))
        .build_decoder()
        .unwrap();
    decoder.serialize(&rows).unwrap();

    let batch: arrow::array::RecordBatch = decoder.flush().unwrap().unwrap();
    Ok(batch)
}
