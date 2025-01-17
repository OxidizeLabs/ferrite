use crate::common::db_instance::DBInstance;
use crate::server::connection::handle_connection;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct ServerHandle {
    port: u16,
    runtime: Option<Runtime>,
    join_handle: Option<JoinHandle<()>>,
}

impl ServerHandle {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            runtime: Some(Runtime::new().unwrap()),
            join_handle: None,
        }
    }

    pub fn start(&mut self, db: Arc<DBInstance>) -> Result<(), Box<dyn std::error::Error>> {
        let port = self.port;
        let runtime = self.runtime.as_ref().unwrap();
        let runtime_handle = Arc::new(runtime.handle().clone());

        let handle = std::thread::spawn({
            let runtime_handle = runtime_handle.clone();
            move || {
                runtime_handle.block_on(async {
                    let addr = format!("127.0.0.1:{}", port);
                    let listener = TcpListener::bind(&addr).await.unwrap();
                    println!("Server listening on {}", addr);

                    loop {
                        match listener.accept().await {
                            Ok((stream, addr)) => {
                                println!("New connection from: {}", addr);
                                let db = Arc::clone(&db);
                                let handle = Arc::clone(&runtime_handle);
                                
                                tokio::task::spawn_blocking(move || {
                                    handle.block_on(async {
                                        handle_connection(stream, db).await;
                                    });
                                });
                            }
                            Err(e) => eprintln!("Error accepting connection: {}", e),
                        }
                    }
                });
            }
        });

        self.join_handle = Some(handle);
        Ok(())
    }

    pub fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(10));
        }
        
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
        
        Ok(())
    }
}
