#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("burst-worker booted");
}
