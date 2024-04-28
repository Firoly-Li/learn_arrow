use arrow_flight::{BasicAuth, FlightClient};
use clap::Parser;
use prost::bytes::BytesMut;

use super::Executor;
use prost::Message;

/**
 *   Handshake命令的参数
 *  handshake -u user -p password
 */
#[derive(Debug, Parser)]
#[command(name = "handshake",version,author,about,long_about = None)]
pub struct Handshake {
    #[arg(short, long)]
    pub user: String,
    // 密码
    #[arg(short, long)]
    pub password: String,
}

impl Executor for Handshake {
    async fn execute(&self, client: &mut FlightClient){
        let mut buf = BytesMut::new();
        let auth: BasicAuth = self.into();
        let _ = auth.encode(&mut buf);
        let response = client
            .handshake(buf.freeze())
            .await
            .expect("--------------------------------------------------");
        println!("{:?}", String::from_utf8(response.to_vec()).unwrap());
    }
}

impl From<&Handshake> for BasicAuth {
    fn from(value: &Handshake) -> Self {
        Self {
            username: value.user.to_string(),
            password: value.password.to_string(),
        }
    }
}
