use arrow_flight::flight_service_server::FlightServiceServer;
use server::FlightServiceImpl;
use tonic::transport::Server;



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_flight_server = FlightServiceImpl::default();
        
    let addr = "[::1]:50051".parse()?;
    let server = FlightServiceServer::new(test_flight_server);
    Server::builder().add_service(server).serve(addr).await?;
    Ok(())
}
