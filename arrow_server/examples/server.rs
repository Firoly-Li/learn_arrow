use arrow_server::flight_server;

#[tokio::main]
async fn main() {
    let _ = flight_server().await;
}
