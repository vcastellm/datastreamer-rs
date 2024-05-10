mod stream_client;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .init();

    let mut server = stream_client::StreamClient::new("stream.zkevm-rpc.com:6900".to_string())
        .expect("Failed to create StreamClient");

    _ = server.start().await;
}
