use axum::Router;
use tokio::task::JoinHandle;

pub struct Server {
    pub base: String,
    handle: JoinHandle<()>,
}

pub async fn serve(router: Router) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a free port");
    let base = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("server error");
    });
    Server { base, handle }
}

impl Server {
    pub fn stop(self) {
        self.handle.abort();
    }
}
