use arrow::{datatypes::Schema, ipc::writer::IpcWriteOptions};
use arrow_flight::{SchemaAsIpc, SchemaResult};
use datafusion::{common::DFSchema, execution::{context::SessionContext, options::ParquetReadOptions}};
use prost::{bytes::{Bytes, BytesMut}, Message};
use tonic::{Response, Status};

/**
 * 根据path获取schema
 */
pub async fn get_schema_by_path(paths: Vec<String>) -> Result<Response<SchemaResult>, Status> {
    // path 可以是tssp文件的路径，可以通过Parquet读取指定tssp文件获取schema信息
    let ctx = SessionContext::new();
    let mut options = ParquetReadOptions::default();
    options.file_extension = "tssp";
    if let Ok(df) = ctx.read_parquet(&paths[0], options).await {
        let schema: Schema = df.schema().into();

        // encode the schema
        let options = IpcWriteOptions::default();
        let response: SchemaResult = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .expect("Error encoding schema");
        Ok(Response::new(response))
    } else {
        Err(Status::ok("message"))
    }
}

/**
 * 根据cmd获取schema
 */
pub async fn get_schema_by_cmd(cmd: Bytes) -> Result<Response<SchemaResult>, Status> {
    let file_name = String::from_utf8_lossy(&cmd.to_vec()).into_owned();
    let path = "/Users/firoly/Documents/code/rust/learn_rust/learn_arrow/datafusion/server/test/".to_string();
    let file_path = path + file_name.as_str();
    let ctx = SessionContext::new();
    let mut options = ParquetReadOptions::default();
    options.file_extension = "tssp";
    let df = ctx.read_parquet(file_path, options).await.unwrap();
    let schema: Schema = df.schema().into();
    println!("schema: {:?}", schema);
    let options = IpcWriteOptions::default();
    let response: SchemaResult = SchemaAsIpc::new(&schema, &options)
        .try_into()
        .expect("Error encoding schema");
    Ok(Response::new(response))
}
