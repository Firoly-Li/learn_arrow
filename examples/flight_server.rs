use std::sync::{Arc, Mutex};

use arrow::{array::RecordBatch, datatypes::Schema, ipc::writer::IpcWriteOptions};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use prost::{bytes::Bytes, Message};
use serde::{Deserialize, Serialize};
use tonic::{metadata::MetadataMap, transport::Server, Request, Response, Status, Streaming};

use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService, FlightServiceServer},
    utils::flight_data_to_batches,
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_flight_server = TestFlightServer::new();
        
    let addr = "[::1]:50051".parse()?;
    let server = FlightServiceServer::new(test_flight_server);
    Server::builder().add_service(server).serve(addr).await?;
    Ok(())
}

fn create_handshake_response(_handshake_request: &HandshakeRequest) -> HandshakeResponse {
    let handshake_response = HandshakeResponse { 
        protocol_version: 1, 
        payload: Bytes::from("hello") 
    };
    handshake_response
}




#[derive(Debug, Clone)]
/// Flight server for testing, with configurable responses
pub struct TestFlightServer {
    /// Shared state to configure responses
    state: Arc<Mutex<State>>,
}

impl TestFlightServer {
    /// Create a `TestFlightServer`
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
        }
    }

    /// Return an [`FlightServiceServer`] that can be used with a
    /// [`Server`](tonic::transport::Server)
    pub fn service(&self) -> FlightServiceServer<TestFlightServer> {
        // wrap up tonic goop
        FlightServiceServer::new(self.clone())
    }

    /// Save the last request's metadatacom
    fn save_metadata<T>(&self, request: &Request<T>) {
        let metadata = request.metadata().clone();
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.last_request_metadata = Some(metadata);
    }
}

/// mutable state for the TestFlightServer, captures requests and provides responses
#[derive(Debug, Default)]
struct State {
    /// The last handshake request that was received
    pub handshake_request: Option<HandshakeRequest>,
    /// The next response to return from `handshake()`
    pub handshake_response: Option<Result<HandshakeResponse, Status>>,
    /// The last `get_flight_info` request received
    pub get_flight_info_request: Option<FlightDescriptor>,
    /// The next response to return from `get_flight_info`
    pub get_flight_info_response: Option<Result<FlightInfo, Status>>,
    /// The last `poll_flight_info` request received
    pub poll_flight_info_request: Option<FlightDescriptor>,
    /// The next response to return from `poll_flight_info`
    pub poll_flight_info_response: Option<Result<PollInfo, Status>>,
    /// The last do_get request received
    pub do_get_request: Option<Ticket>,
    /// The next response returned from `do_get`
    pub do_get_response: Option<Vec<Result<RecordBatch, Status>>>,
    /// The last do_put request received
    pub do_put_request: Option<Vec<FlightData>>,
    /// The next response returned from `do_put`
    pub do_put_response: Option<Vec<Result<PutResult, Status>>>,
    /// The last do_exchange request received
    pub do_exchange_request: Option<Vec<FlightData>>,
    /// The next response returned from `do_exchange`
    pub do_exchange_response: Option<Vec<Result<FlightData, Status>>>,
    /// The last list_flights request received
    pub list_flights_request: Option<Criteria>,
    /// The next response returned from `list_flights`
    pub list_flights_response: Option<Vec<Result<FlightInfo, Status>>>,
    /// The last get_schema request received
    pub get_schema_request: Option<FlightDescriptor>,
    /// The next response returned from `get_schema`
    pub get_schema_response: Option<Result<Schema, Status>>,
    /// The last list_actions request received
    pub list_actions_request: Option<Empty>,
    /// The next response returned from `list_actions`
    pub list_actions_response: Option<Vec<Result<ActionType, Status>>>,
    /// The last do_action request received
    pub do_action_request: Option<Action>,
    /// The next response returned from `do_action`
    pub do_action_response: Option<Vec<Result<arrow_flight::Result, Status>>>,
    /// The last request headers received
    pub last_request_metadata: Option<MetadataMap>,
}

impl State {
    fn new() -> Self {
        Default::default()
    }
}

/// Implement the FlightService trait
#[tonic::async_trait]
impl FlightService for TestFlightServer {
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

        let handshake_request = request.into_inner().message().await?.unwrap();
        let payload = &handshake_request.payload;
        let fd: FlightData = FlightData::decode(payload.clone()).unwrap();
        println!("接收到客户端的消息 FlightData : {:?}", fd);

        // 将 FlightData 转换为 RecordBatch
        match flight_data_to_batches(&[fd]) {
            Ok(re) => {
                println!("RecordBatch: {:?}", re);
            }
            Err(e) => println!("e = {:?}", e),
        };
        // 构建 handshake_response
        let handshake_response = create_handshake_response(&handshake_request);
            
        // 构建 response
        let output = futures::stream::iter(std::iter::once(Ok(handshake_response)));
        Ok(Response::new(output.boxed()))
    }

    /**
     * 客户端可以向服务端发送 ListFlights 请求，服务端响应包含可用数据集或服务列表的 FlightInfo 对象。
     * 这些信息通常包括数据集的描述、Schema、分区信息等，帮助客户端了解可访问的数据资源。
     */
    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");

        state.list_flights_request = Some(request.into_inner());

        let flights: Vec<_> = state
            .list_flights_response
            .take()
            .ok_or_else(|| Status::internal("No list_flights response configured"))?;

        let flights_stream = futures::stream::iter(flights);

        Ok(Response::new(flights_stream.boxed()))
    }

    /**
     * 客户端请求特定数据集的详细信息，服务端返回 FlightInfo，
     * 其中包含数据集的完整 Schema、数据分布情况（如有多个分片）、访问凭证（如有）等
     */
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.get_flight_info_request = Some(request.into_inner());
        let response = state
            .get_flight_info_response
            .take()
            .unwrap_or_else(|| Err(Status::internal("No get_flight_info response configured")))?;
        Ok(Response::new(response))
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.poll_flight_info_request = Some(request.into_inner());
        let response = state
            .poll_flight_info_response
            .take()
            .unwrap_or_else(|| Err(Status::internal("No poll_flight_info response configured")))?;
        Ok(Response::new(response))
    }

    /**
     * 客户端请求某个数据集的Schema信息，服务端返回详细的Schema定义，便于客户端正确解析接收到的数据。
     */
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.get_schema_request = Some(request.into_inner());
        let schema = state
            .get_schema_response
            .take()
            .unwrap_or_else(|| Err(Status::internal("No get_schema response configured")))?;

        // encode the schema
        let options = IpcWriteOptions::default();
        let response: SchemaResult = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .expect("Error encoding schema");

        Ok(Response::new(response))
    }

    /**
     * 客户端根据 FlightInfo 发送 DoGet 请求以获取数据。服务端以流的形式返回数据，
     * 每个数据包通常是 Arrow 格式的 RecordBatch。
     * 客户端可以逐步接收和处理这些数据包，无需一次性加载全部数据。
     */
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");

        state.do_get_request = Some(request.into_inner());

        let batches: Vec<_> = state
            .do_get_response
            .take()
            .ok_or_else(|| Status::internal("No do_get response configured"))?;

        let batch_stream = futures::stream::iter(batches).map_err(Into::into);

        let stream = FlightDataEncoderBuilder::new()
            .build(batch_stream)
            .map_err(Into::into);

        let mut resp = Response::new(stream.boxed());
        resp.metadata_mut()
            .insert("test-resp-header", "some_val".parse().unwrap());

        Ok(resp)
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
        self.save_metadata(&request);
        println!("request = {:?}", request);
        let do_put_request: Vec<_> = request.into_inner().try_collect().await?;

        let mut state = self.state.lock().expect("mutex not poisoned");

        state.do_put_request = Some(do_put_request);

        let response = state
            .do_put_response
            .take()
            .ok_or_else(|| Status::internal("No do_put response configured"))?;

        let stream = futures::stream::iter(response).map_err(Into::into);

        Ok(Response::new(stream.boxed()))
    }

    /**
     * 客户端可以发送一个包含特定操作请求的消息（如执行 SQL 查询、触发数据处理任务等）。
     * 服务端执行相应操作，并返回操作结果或状态信息。
     * 此机制扩展了 Arrow Flight 的功能，使其不仅局限于数据传输，还能支持复杂的业务逻辑
     */
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");

        state.do_action_request = Some(request.into_inner());

        let results: Vec<_> = state
            .do_action_response
            .take()
            .ok_or_else(|| Status::internal("No do_action response configured"))?;

        let results_stream = futures::stream::iter(results);

        Ok(Response::new(results_stream.boxed()))
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");

        state.list_actions_request = Some(request.into_inner());

        let actions: Vec<_> = state
            .list_actions_response
            .take()
            .ok_or_else(|| Status::internal("No list_actions response configured"))?;

        let action_stream = futures::stream::iter(actions);

        Ok(Response::new(action_stream.boxed()))
    }

    /**
     * 支持客户端和服务端之间进行双向数据流交换。
     * 双方可以同时发送和接收 Arrow RecordBatch，
     * 适用于需要实时交互或迭代计算的场景，如查询中间结果的反馈、增量计算等
     */
    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.save_metadata(&request);
        let do_exchange_request: Vec<_> = request.into_inner().try_collect().await?;

        let mut state = self.state.lock().expect("mutex not poisoned");

        state.do_exchange_request = Some(do_exchange_request);

        let response = state
            .do_exchange_response
            .take()
            .ok_or_else(|| Status::internal("No do_exchange response configured"))?;

        let stream = futures::stream::iter(response).map_err(Into::into);

        Ok(Response::new(stream.boxed()))
    }
}




#[derive(Serialize,Deserialize)]
struct MyStruct {
    int32: i32,
    string: String,
}



#[cfg(test)]
mod tests {
    use arrow_flight::FlightData;

    fn create_flight_data() {
        FlightData::from(value)
    }

    #[test]
    fn test() {}
}
