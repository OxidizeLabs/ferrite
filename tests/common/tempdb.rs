use tkdb::common::db_instance::{DBConfig, DBInstance};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use std::error::Error;

fn unique_suffix() -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{}", nanos)
}

pub fn temp_db_config() -> DBConfig {
    let temp_dir = PathBuf::from("tests/temp");
    let _ = std::fs::create_dir_all(&temp_dir);
    let suffix = unique_suffix();
    let db_filename = temp_dir.join(format!("tkdb_{}.db", suffix));
    let log_filename = temp_dir.join(format!("tkdb_{}.log", suffix));
    DBConfig {
        db_filename: db_filename.to_string_lossy().to_string(),
        db_log_filename: log_filename.to_string_lossy().to_string(),
        buffer_pool_size: 256,
        enable_logging: true,
        enable_managed_transactions: true,
        lru_k: 10,
        lru_sample_size: 7,
        server_enabled: false,
        server_host: "127.0.0.1".to_string(),
        server_port: 5432,
        max_connections: 32,
        connection_timeout: 10,
    }
}

pub async fn new_temp_db() -> Result<DBInstance, Box<dyn Error>> {
    let cfg = temp_db_config();
    Ok(DBInstance::new(cfg).await?)
}

pub fn cleanup_temp_artifacts() {
    let temp_dir = PathBuf::from("tests/temp");
    if temp_dir.exists() {
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}


