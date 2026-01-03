//! Crash recovery evidence demo.
//!
//! This example intentionally aborts the process to simulate a crash and then verifies
//! ARIES-style recovery on restart.
//!
//! Usage:
//!   cargo run --example crash_recovery_evidence -- setup  /path/to/db.db /path/to/db.log
//!   cargo run --example crash_recovery_evidence -- crash  /path/to/db.db /path/to/db.log
//!   cargo run --example crash_recovery_evidence -- verify /path/to/db.db /path/to/db.log

use std::env;
use std::sync::Arc;

use ferrite::common::db_instance::{DBConfig, DBInstance};
use ferrite::common::result_writer::ResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;
use ferrite::types_db::value::{Val, Value};

#[derive(Default)]
struct CaptureWriter {
    rows: Vec<Vec<Value>>,
}

impl CaptureWriter {
    fn new() -> Self {
        Self { rows: Vec::new() }
    }

    fn rows(&self) -> &[Vec<Value>] {
        &self.rows
    }
}

impl ResultWriter for CaptureWriter {
    fn write_schema_header(&mut self, _headers: Vec<String>) {}
    fn write_row(&mut self, values: Vec<Value>) {
        self.rows.push(values);
    }
    fn write_row_with_schema(
        &mut self,
        values: Vec<Value>,
        _schema: &ferrite::catalog::schema::Schema,
    ) {
        self.rows.push(values);
    }
    fn write_message(&mut self, _message: &str) {}
}

fn usage_and_exit() -> ! {
    eprintln!(
        "Usage:\n  crash_recovery_evidence <setup|crash|verify> <db_path> <log_path>\n\nExamples:\n  cargo run --example crash_recovery_evidence -- setup  /tmp/ferrite-evidence.db /tmp/ferrite-evidence.log\n  cargo run --example crash_recovery_evidence -- crash  /tmp/ferrite-evidence.db /tmp/ferrite-evidence.log\n  cargo run --example crash_recovery_evidence -- verify /tmp/ferrite-evidence.db /tmp/ferrite-evidence.log"
    );
    std::process::exit(2);
}

fn build_config(db_path: &str, log_path: &str) -> DBConfig {
    DBConfig {
        db_filename: db_path.to_string(),
        db_log_filename: log_path.to_string(),
        enable_logging: true,
        enable_managed_transactions: true,
        buffer_pool_size: 64,
        ..Default::default()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        usage_and_exit();
    }

    let mode = &args[1];
    let db_path = &args[2];
    let log_path = &args[3];

    match mode.as_str() {
        "setup" => setup_mode(db_path, log_path).await,
        "crash" => crash_mode(db_path, log_path).await,
        "verify" => verify_mode(db_path, log_path).await,
        _ => usage_and_exit(),
    }
}

async fn setup_mode(db_path: &str, log_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = build_config(db_path, log_path);
    let db = DBInstance::new(cfg).await?;

    // Create the table in a non-crashing run so the catalog is durable before we simulate a crash.
    let mut w = CaptureWriter::new();
    db.execute_sql(
        "CREATE TABLE evidence (id INTEGER, value VARCHAR);",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await?;

    // Ensure the system catalog/table metadata reaches disk even though this example exits immediately.
    // (Writes may be buffered inside the async disk manager; use the durable flush helper.)
    db.get_buffer_pool_manager()
        .flush_all_pages_durable()
        .await?;

    println!("[evidence] setup complete");
    Ok(())
}

async fn crash_mode(db_path: &str, log_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = build_config(db_path, log_path);
    let db = Arc::new(DBInstance::new(cfg).await?);

    // 1) Commit row id=1 (auto-commit via execute_sql).
    let mut w = CaptureWriter::new();
    db.execute_sql(
        "INSERT INTO evidence VALUES (1, 'committed');",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await?;

    // 2) Insert id=2 inside an explicit transaction, but DO NOT commit.
    let txn = db.begin_transaction(IsolationLevel::Serializable);
    db.execute_transaction(
        "INSERT INTO evidence VALUES (2, 'uncommitted');",
        txn.clone(),
        &mut w,
    )
    .await?;

    // 3) Simulate a hard crash (no unwinding, no Drop).
    eprintln!("[evidence] Simulating crash now (process abort)...");
    std::process::abort();
}

async fn verify_mode(db_path: &str, log_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = build_config(db_path, log_path);
    let db = DBInstance::new(cfg).await?;

    // Committed row must exist.
    let mut w1 = CaptureWriter::new();
    db.execute_sql(
        "SELECT id FROM evidence WHERE id = 1;",
        IsolationLevel::ReadCommitted,
        &mut w1,
    )
    .await?;
    if w1.rows().len() != 1 {
        return Err(format!(
            "expected committed row to exist; got {} rows",
            w1.rows().len()
        )
        .into());
    }
    match &w1.rows()[0][0].value_ {
        Val::Integer(1) => {},
        other => return Err(format!("expected id=1, got {other:?}").into()),
    }

    // Uncommitted row must NOT exist (should be undone during recovery).
    let mut w2 = CaptureWriter::new();
    db.execute_sql(
        "SELECT id FROM evidence WHERE id = 2;",
        IsolationLevel::ReadCommitted,
        &mut w2,
    )
    .await?;
    if !w2.rows().is_empty() {
        return Err(format!(
            "expected uncommitted row to be absent; got {} rows",
            w2.rows().len()
        )
        .into());
    }

    println!(
        "[evidence] OK: crash recovery preserved committed work and rolled back uncommitted work."
    );
    Ok(())
}
