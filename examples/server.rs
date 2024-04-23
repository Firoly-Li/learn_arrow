use learn_arrow::hw::helloworld::{
    greeter_server::{Greeter, GreeterServer},
    HelloReply, HelloRequest,
};
use prost::bytes::Bytes;
use tonic::{transport::Server, Request, Response, Status};
#[derive(Debug, Default)]
pub struct MyGreeter;

//
#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Received a request: {:#?}", request);

        let reply = HelloReply {
            message: format!("Hello {}!", request.into_inner().name),
            arrow_msg: Bytes::from("bytes").to_vec(),
        };
        // 实现业务逻辑
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    let greeter = MyGreeter::default();

    Server::builder()
        .add_service(GreeterServer::new(greeter)) // 注册服务
        .serve(addr)
        .await?;

    Ok(())
}
