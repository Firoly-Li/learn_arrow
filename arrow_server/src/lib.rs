use anyhow::{Ok, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use flight::FlightServiceImpl;
use tonic::transport::Server;

pub mod client;
mod data_fusion;
mod flight;
mod parquet;
mod web;

/**
 * 启动flight_server
 */
pub async fn flight_server() -> Result<()> {
    let test_flight_server = FlightServiceImpl::default();
    let addr = "[::1]:50051".parse()?;
    let server = FlightServiceServer::new(test_flight_server);
    println!("flight server will be starting on :{}", addr);
    Server::builder().add_service(server).serve(addr).await?;

    Ok(())
}
