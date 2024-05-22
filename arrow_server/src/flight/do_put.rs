
use arrow::{array::RecordBatch, datatypes::ArrowNativeType};
use arrow_flight::PutResult;
use parquet::{arrow::AsyncArrowWriter, basic::Compression, data_type::AsBytes, file::properties::WriterProperties};
use prost::bytes::Bytes;
use tonic::Status;

pub async fn write_batch(batchs: Vec<RecordBatch>) -> Vec<Result<PutResult, Status>> {
        let mut resp = Vec::new();
        let mut file_index = 0;
        for batch in batchs {
            let file_name = file_index.to_string();
            let path = "/Users/firoly/Documents/code/rust/learn_rust/learn_arrow/datafusion/server/test/".to_string() + file_name.as_str() + ".tssp";
            println!("path = {}", path);
            let file = tokio::fs::File::create(path).await.unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer = AsyncArrowWriter::try_new(file, batch.schema().clone(), Some(props)).unwrap();
            for _i in 0..100 {
                writer.write(&batch).await.unwrap();
            }
            writer.close().await.unwrap();
            
            resp.push(Ok(PutResult {
                app_metadata: Bytes::from(vec![file_index]),
            }));
            file_index += 1;
        }
        resp
}
