use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    json::ReaderBuilder,
};

use arrow_flight::{utils::batches_to_flight_data, FlightClient, FlightData};

use prost::bytes::BytesMut;
use prost::Message;
use serde::Serialize;
use tonic::transport::Channel;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await
        .expect("error connecting");

    let mut client = FlightClient::new(channel);

    if let Ok(batch) = create_batch() {
        let mut buf = BytesMut::new();
        let fd: FlightData = batch[0].clone();
        let _ = fd.encode(&mut buf);
        let bytes = buf.freeze();
        let response = client
            .handshake(bytes)
            .await
            .expect("--------------------------------------------------");
        println!("response = {:?}", response);
    }
    Ok(())
}

#[derive(Serialize)]
struct MyStruct {
    int32: i32,
    string: String,
}
fn create_batch() -> Result<Vec<FlightData>, ArrowError> {
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
        array::RecordBatch, datatypes::{DataType, Field, Schema}, json::ReaderBuilder
    };
    use arrow_flight::{utils::{batches_to_flight_data, flight_data_to_batches}, FlightData};
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
    fn record_batch_to_flight_data_test(){
        let batch = create_record_batch();
        println!("batch = {:?}", batch);
        let batches = vec![batch.clone()];
        let fd = batches_to_flight_data(&batch.schema(),batches).unwrap();
        println!("flight_data = {:?}", fd);
    }

    #[test]
    fn record_batch_exchange_flight_data_test() {
        // 1. create a record batch
        let batch = create_record_batch();
        println!("batch = {:?}", batch);
        // 2. convert record batch to flight data
        let batches = vec![batch.clone()];
        let fd = batches_to_flight_data(&batch.schema(),batches).unwrap();
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
