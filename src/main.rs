mod stream_client;

#[tokio::main]
async fn main() {
    let server = stream_client::StreamClient::new("localhost:6900".to_string());

    _ = server.expect("Failt to create").start().await;
}
