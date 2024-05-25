use arrow::{datatypes::Schema, ipc::writer::IpcWriteOptions};
use arrow_flight::{SchemaAsIpc, SchemaResult};
use datafusion::{
    common::DFSchema,
    execution::{context::SessionContext, options::ParquetReadOptions},
};
use prost::{
    bytes::{Bytes, BytesMut},
    Message,
};
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
 * todo cmd格式：标准sql
 * 根据CMD查询具体
 */
pub async fn get_schema_by_cmd(cmd: Bytes) -> Result<Response<SchemaResult>, Status> {
    let ctx = SessionContext::new();
    let sql = String::from_utf8(cmd.to_vec()).unwrap();
    let resp = ctx.sql(&sql).await;
    Ok(Response::new(SchemaResult::default()))
}
