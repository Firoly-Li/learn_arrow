pub mod get_schema;
pub mod handshake;
mod put;

use self::{get_schema::GetSchema, handshake::Handshake};
use arrow_flight::FlightClient;
use clap::Parser;

#[derive(Debug, Parser)]
pub enum SubCmd {
    Handshake(Handshake),
    GetSchema(GetSchema),
}

#[allow(async_fn_in_trait)]

pub trait Executor {
    async fn execute(&self, client: &mut FlightClient);
}

impl Executor for SubCmd {
    async fn execute(&self, client: &mut FlightClient) {
        match self {
            SubCmd::Handshake(handshake) => handshake.execute(client).await,
            SubCmd::GetSchema(schema) => schema.execute(client).await,
        }
    }
}
