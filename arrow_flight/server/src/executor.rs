use arrow_flight::{HandshakeRequest, HandshakeResponse};
use prost::bytes::Bytes;


/**
 * 执行器
 */
pub trait Executor {
    type Response;

    fn execute(&self) -> Result<Self::Response, String>;
}


/**
 * HandshakeRequest 执行器
 */
impl Executor for HandshakeRequest {
    type Response= HandshakeResponse;

    fn execute(&self) -> Result<Self::Response, String> {
        let handshake_response = HandshakeResponse { 
            protocol_version: 1, 
            payload: Bytes::from("hello") 
        };
        Ok(handshake_response)
    }
}
