use std::{
    borrow::{Borrow, BorrowMut},
    fs::File,
    sync::Arc,
};

use arrow::{array::RecordBatch, datatypes::Schema, ipc::writer::IpcWriteOptions};
use arrow_flight::{
    flight_descriptor::DescriptorType,
    flight_service_server::FlightService,
    utils::{flight_data_to_arrow_batch, flight_data_to_batches},
    Action, ActionType, BasicAuth, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use datafusion::{
    execution::{context::SessionContext, options::ParquetReadOptions},
    logical_expr::{tree_node::plan, LogicalPlanBuilder},
    physical_plan::stream,
};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use prost::{bytes::Bytes, Message};
use tonic::{Request, Response, Status, Streaming};

#[derive(Debug, Default, Clone)]
pub struct FlightServiceImpl {}

impl FlightServiceImpl {
    fn write_parquet(&self, batches: Vec<RecordBatch>) {
        let schema = batches.first().unwrap().schema();
        let path = "";
        // 写入 Parquet 文件
        let file = File::create(path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.into(), Some(props)).unwrap();
        for batch in batches {
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();
    }
}

/**
 *
 */
#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        // 1. 获取 HandshakeRequest
        let handshake_request: HandshakeRequest = request.into_inner().message().await?.unwrap();
        println!(
            "接收到客户端的HandshakeRequest报文：{:?}",
            handshake_request
        );
        let _version = handshake_request.protocol_version;
        let p = handshake_request.payload;
        let auth = BasicAuth::decode(p);
        println!("BasicAuth{:?}", auth);
        match auth {
            Ok(a) => {
                println!("客户端认证信息：{:?}", a);
                let handshake_response = HandshakeResponse {
                    protocol_version: 1,
                    payload: Bytes::from("auth success"),
                };
                let output = futures::stream::iter(std::iter::once(Ok(handshake_response)));
                Ok(Response::new(output.boxed()))
            }
            Err(_e) => Err(Status::ok("message")),
        }
    }

    /**
     * 客户端可以向服务端发送 ListFlights 请求，服务端响应包含可用数据集或服务列表的 FlightInfo 对象。
     * 这些信息通常包括数据集的描述、Schema、分区信息等，帮助客户端了解可访问的数据资源。
     */
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::ok("message"))
    }

    /**
     * 客户端请求特定数据集的详细信息，服务端返回 FlightInfo，
     * 其中包含数据集的完整 Schema、数据分布情况（如有多个分片）、访问凭证（如有）等
     */
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::ok("message"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::ok("message"))
    }

    /**
     * 客户端请求某个数据集的Schema信息，服务端返回详细的Schema定义，便于客户端正确解析接收到的数据。
     */
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let request = request.into_inner();
        let desc_type = request.r#type();
        match desc_type {
            DescriptorType::Unknown => Err(Status::ok("message")),
            DescriptorType::Path => {
                let paths = request.path;
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
            DescriptorType::Cmd => Err(Status::ok("message")),
        }
    }

    /**
     * 客户端根据 FlightInfo 发送 DoGet 请求以获取数据。服务端以流的形式返回数据，
     * 每个数据包通常是 Arrow 格式的 RecordBatch。
     * 客户端可以逐步接收和处理这些数据包，无需一次性加载全部数据。
     * 
     * Ticket：
     * Ticket是一个用于请求数据的凭据或者说是标识。当客户端想要从Flight服务器获取数据时，
     * 它会发送一个包含Ticket的请求给服务器。这个Ticket实质上包含了服务器所需的所有信息
     * 以便准确地定位和检索请求的数据集。
     * 具体来说，Ticket可以包含例如表名、查询ID、数据过滤条件或者其他任何必要的元数据，
     * 这些都是为了能够让服务器识别并处理这个请求，从而返回相应的数据给客户端。
     * 在Flight的协议层面，Ticket是一个二进制序列化的对象，其具体内容和格式取决于实现和使用的上下文。
     */
    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::ok("message"))
    }

    /**
     * 客户端使用 DoPut 请求将数据上传至服务端。
     * 客户端以流的形式发送包含 Arrow RecordBatch 的数据包，服务端接收并存储这些数据。
     * 这种方式常用于数据导入或实时数据流传输。
     */
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let stream: Vec<_> = request.into_inner().collect().await;
        // 从第一个FlightData消息中尝试提取schema
        let ss = stream.into_iter().filter_map(|fd|fd.ok()).collect::<Vec<FlightData>>();
        let batches = flight_data_to_batches(ss.borrow()).unwrap();
        // 这里需要把RecordBatch存储起来，以便于后续使用
        Err(Status::ok("put success!"))
    }

    /**
     * 用于扩展arrowFlight机制，可以自定义Action
     * 客户端可以发送一个包含特定操作请求的消息（如执行 SQL 查询、触发数据处理任务等）。
     * 服务端执行相应操作，并返回操作结果或状态信息。
     * 此机制扩展了 Arrow Flight 的功能，使其不仅局限于数据传输，还能支持复杂的业务逻辑
     */
    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::ok("message"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::ok("message"))
    }

    /**
     * 支持客户端和服务端之间进行双向数据流交换。
     * 双方可以同时发送和接收 Arrow RecordBatch，
     * 适用于需要实时交互或迭代计算的场景，如查询中间结果的反馈、增量计算等
     */
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::ok("message"))
    }
}
