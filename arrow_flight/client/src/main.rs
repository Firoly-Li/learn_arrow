use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    json::ReaderBuilder,
};

use arrow_flight::{
    utils::batches_to_flight_data, FlightClient, FlightData, FlightDescriptor, Ticket,
};

use client::{do_list_flights, do_put_test};
use futures::StreamExt;
use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use serde::Serialize;
use tonic::transport::Channel;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let remote_url = "http://192.168.3.223:50051";
    let local_url = "http://127.0.0.1:50051";
    if let Ok(channel) = Channel::from_static(local_url).connect().await {
        let mut client = FlightClient::new(channel);
        let resp = do_put_test(&mut client).await;
    } else {
        println!("客户端连接失败！");
    }
    Ok(())
}

/**
 * 测试握手协议
 */
async fn test_handshake(client: &mut FlightClient) {
    if let Ok(batch) = create_batch() {
        let mut buf = BytesMut::new();
        let fd: FlightData = batch[0].clone();
        let _ = fd.encode(&mut buf);
        let bytes = buf.freeze();
        // let request = HandshakeRequest{
        //     protocol_version: todo!(),
        //     payload: todo!(),
        // };
        let response = client
            .handshake(bytes)
            .await
            .expect("--------------------------------------------------");
        println!("服务端返回的消息： {:?}", response);
    }
}

async fn test_get_schema(client: &mut FlightClient) {
    let desc = FlightDescriptor::new_cmd("0.tssp".as_bytes());
    let response = client.get_schema(desc).await;
    println!("服务端返回的消息： {:?}", response);
}

/**
 * 测试获取数据
 */
async fn test_do_get(client: &mut FlightClient) {
    let ticket = Ticket {
        ticket: Bytes::from_static("0".as_bytes()),
    };
    let mut resp = client
        .do_get(ticket)
        .await
        .expect("--------------------------------------------------");
    println!("resp: {:?}", resp);
    loop {
        let r = resp.next().await;
        if let Some(batch) = r {
            println!("batch: {:?}", batch);
        } else {
            break;
        }
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

    let batches = vec![batch];

    batches_to_flight_data(&schema, batches)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema},
        json::ReaderBuilder,
    };
    use arrow_flight::utils::{batches_to_flight_data, flight_data_to_batches};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct MyStruct {
        int32: i32,
        string: String,
    }

    #[test]
    fn test() {
        let batch = create_record_batch();
        println!("batch = {:?}", batch);
    }

    fn create_record_batch() -> arrow::array::RecordBatch {
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

    #[test]
    fn record_batch_to_flight_data_test() {
        let batch = create_record_batch();
        println!("batch = {:?}", batch);
        let batches = vec![batch.clone()];
        let fd = batches_to_flight_data(&batch.schema(), batches).unwrap();
        println!("flight_data = {:?}", fd);
    }

    #[test]
    fn record_batch_exchange_flight_data_test() {
        // 1. create a record batch
        let batch = create_record_batch();
        println!("batch = {:?}", batch);
        // 2. convert record batch to flight data
        let batches = vec![batch.clone()];
        let fd = batches_to_flight_data(&batch.schema(), batches).unwrap();
        println!("flight_data = {:?}", fd);

        // 3. exchange flight data to record batch
        let batch: Vec<RecordBatch> = flight_data_to_batches(&fd).unwrap();

        println!("bacth = {:?}", batch);

        //RecordBatch {
        //    schema: Schema {
        //      fields: [
        //        Field { name: "int32", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
        //        Field { name: "string", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }
        //      ],
        //      metadata: {}
        //    },
        //    columns: [
        //      PrimitiveArray<Int32>[5,8,],
        //      StringArray["bar","foo",]
        //    ],
        //    row_count: 2
        //  }
    }
}
