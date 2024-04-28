use arrow_flight::{FlightClient, FlightDescriptor};
use clap::Parser;
use prost::bytes::Bytes;

use super::Executor;


#[derive(Debug, Parser)]
#[command(name = "schema",version,author,about,long_about = None)]
pub struct GetSchema  {
    #[arg(short, long)]
    pub cmd: Option<String>,
    #[arg(short, long)]
    pub path: Option<String>,
}



impl Executor for GetSchema {
    
    async fn execute(&self, client: &mut FlightClient){
        let desc = if self.cmd.is_none(){
            let path = self.path.as_ref().unwrap();
            FlightDescriptor::new_path(vec![path.to_string()]) 
        }else {
            let cmd = self.cmd.as_ref().unwrap();
            FlightDescriptor::new_cmd(cmd.to_string())
        };
        let resp = client
        .get_schema(desc)
        .await
        .expect("msg");
        println!("schema: {:?}", resp);
    }
}