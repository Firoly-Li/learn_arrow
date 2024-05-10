use std::{
    default,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Ok, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use flight::FlightServiceImpl;
use tonic::transport::Server;

pub mod client;
pub mod data_fusion;
pub mod error;
pub mod flight;
pub mod parquet;
pub mod web;

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

pub fn now() -> usize {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as usize
}
