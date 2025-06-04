use sqllogictest::{DBOutput, DefaultColumnType, DB};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tkdb::common::db_instance::{DBConfig, DBInstance};
use tkdb::common::exception::DBError;
use tkdb::common::result_writer::ResultWriter;
use tkdb::common::logger;
use tkdb::concurrency::transaction::IsolationLevel;
use tkdb::types_db::value::Value;
use tkdb::catalog::schema::Schema;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use sqllogictest::Runner;
use std::sync::Once;

// Add color constants at the top of the file
const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const BLUE: &str = "\x1b[34m";
const RESET: &str = "\x1b[0m";

#[derive(Default, Clone)]
struct TestStats {
    files_run: usize,
    statements_run: usize,
    queries_run: usize,
    errors_caught: usize,
    duration: Duration,
    successful_statements: usize,
    successful_queries: usize,
    failed_statements: usize,
    failed_queries: usize,
}

impl TestStats {
    fn new() -> Self {
        Self::default()
    }

    fn print_summary(&self) {
        println!("\n{}Test Summary:{}", BLUE, RESET);
        println!("-------------");
        println!("Files executed:     {}", self.files_run);
        println!("Statements run:     {}", self.statements_run);
        println!(
            "  {}Success:          {} ({:.1}%){}",
            GREEN,
            self.successful_statements,
            if self.statements_run > 0 {
                self.successful_statements as f32 / self.statements_run as f32 * 100.0
            } else {
                0.0
            },
            RESET
        );
        println!(
            "  {}Failed:           {} ({:.1}%){}",
            RED,
            self.failed_statements,
            if self.statements_run > 0 {
                self.failed_statements as f32 / self.statements_run as f32 * 100.0
            } else {
                0.0
            },
            RESET
        );
        println!("Queries executed:   {}", self.queries_run);
        println!(
            "  {}Success:          {} ({:.1}%){}",
            GREEN,
            self.successful_queries,
            if self.queries_run > 0 {
                self.successful_queries as f32 / self.queries_run as f32 * 100.0
            } else {
                0.0
            },
            RESET
        );
        println!(
            "  {}Failed:           {} ({:.1}%){}",
            RED,
            self.failed_queries,
            if self.queries_run > 0 {
                self.failed_queries as f32 / self.queries_run as f32 * 100.0
            } else {
                0.0
            },
            RESET
        );
        println!(
            "{}Errors validated:   {}{}",
            YELLOW, self.errors_caught, RESET
        );
        println!("Total duration:     {:.2?}", self.duration);
        if self.files_run > 0 {
            println!(
                "Average time/file:  {:.2?}",
                self.duration.div_f32(self.files_run as f32)
            );
        } else {
            println!("Average time/file:  N/A (no files executed)");
        }
        println!("-------------\n");
    }
}

/// Generate unique temporary file paths for test isolation
fn generate_temp_db_config() -> DBConfig {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let thread_id = std::thread::current().id();
    
    // Create temp directory if it doesn't exist
    let temp_dir = PathBuf::from("tests/temp");
    fs::create_dir_all(&temp_dir).unwrap_or_else(|e| {
        eprintln!("Warning: Failed to create temp directory: {}", e);
    });
    
    let db_filename = temp_dir.join(format!("test_{}_{:?}.db", timestamp, thread_id));
    let log_filename = temp_dir.join(format!("test_{}_{:?}.log", timestamp, thread_id));
    
    DBConfig {
        db_filename: db_filename.to_string_lossy().to_string(),
        db_log_filename: log_filename.to_string_lossy().to_string(),
        buffer_pool_size: 512, // Smaller buffer pool for tests
        enable_logging: true,
        enable_managed_transactions: true,
        lru_k: 10,
        lru_sample_size: 7,
        server_enabled: false,
        server_host: "127.0.0.1".to_string(),
        server_port: 5432,
        max_connections: 100,
        connection_timeout: 30,
    }
}

/// Clean up temporary database files
fn cleanup_temp_files(config: &DBConfig) {
    let _ = fs::remove_file(&config.db_filename);
    let _ = fs::remove_file(&config.db_log_filename);
}

/// Clean up the entire temp directory after tests
fn cleanup_temp_directory() {
    let temp_dir = PathBuf::from("tests/temp");
    if temp_dir.exists() {
        let _ = fs::remove_dir_all(&temp_dir);
    }
}

pub struct TKDBTest {
    instance: DBInstance,
    config: DBConfig, // Store config for cleanup
    stats: Arc<Mutex<TestStats>>, // Add shared statistics tracking
}

impl TKDBTest {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        let config = generate_temp_db_config();
        Self::new_with_config(config)
    }

    pub fn new_with_config(config: DBConfig) -> Result<Self, Box<dyn Error>> {
        let instance = DBInstance::new(config.clone())?;
        Ok(Self {
            instance,
            config,
            stats: Arc::new(Mutex::new(TestStats::new())),
        })
    }

    fn update_stats(&mut self, output: &DBOutput<DefaultColumnType>, success: bool) {
        let mut stats = self.stats.lock().unwrap();
        match output {
            DBOutput::StatementComplete(_) => {
                stats.statements_run += 1;
                if success {
                    stats.successful_statements += 1;
                } else {
                    stats.failed_statements += 1;
                }
            }
            DBOutput::Rows { .. } => {
                stats.queries_run += 1;
                if success {
                    stats.successful_queries += 1;
                } else {
                    stats.failed_queries += 1;
                }
            }
            _ => {
                // For any other output type, we don't track statistics
            }
        }
    }

    fn get_stats(&self) -> TestStats {
        self.stats.lock().unwrap().clone()
    }
}

impl Drop for TKDBTest {
    fn drop(&mut self) {
        cleanup_temp_files(&self.config);
    }
}

struct TestResultWriter {
    column_names: Vec<String>,
    rows: Vec<Vec<Value>>,
    schema: Option<Schema>,
}

impl TestResultWriter {
    fn new() -> Self {
        Self {
            column_names: Vec::new(),
            rows: Vec::new(),
            schema: None,
        }
    }
}

impl ResultWriter for TestResultWriter {
    fn write_schema_header(&mut self, headers: Vec<String>) {
        self.column_names = headers;
    }

    fn write_row(&mut self, values: Vec<Value>) {
        self.rows.push(values);
    }

    fn write_row_with_schema(&mut self, values: Vec<Value>, schema: &Schema) {
        self.schema = Some(schema.clone());
        self.rows.push(values);
    }

    fn write_message(&mut self, _message: &str) {
        // Ignore messages for testing
    }
}

impl DB for TKDBTest {
    type Error = DBError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut writer = TestResultWriter::new();

        // Execute the query
        match self
            .instance
            .execute_sql(sql, IsolationLevel::ReadCommitted, &mut writer)
        {
            Ok(success) => {
                if success {
                    let output = if writer.column_names.is_empty() {
                        // Statement execution (no results)
                        DBOutput::StatementComplete(0)
                    } else {
                        // Query execution (with results)
                        let rows: Vec<Vec<String>> = writer
                            .rows
                            .into_iter()
                            .map(|row| {
                                if let Some(ref schema) = writer.schema {
                                    // Use schema-aware formatting
                                    row.into_iter()
                                        .enumerate()
                                        .map(|(i, v)| {
                                            if let Some(column) = schema.get_column(i) {
                                                v.format_with_column_context(Some(column))
                                            } else {
                                                v.to_string()
                                            }
                                        })
                                        .collect()
                                } else {
                                    // Fallback to default formatting
                                    row.into_iter().map(|v| v.to_string()).collect()
                                }
                            })
                            .collect();

                        DBOutput::Rows {
                            types: vec![DefaultColumnType::Text; writer.column_names.len()],
                            rows,
                        }
                    };
                    self.update_stats(&output, true);
                    Ok(output)
                } else {
                    let output = DBOutput::StatementComplete(0);
                    self.update_stats(&output, false);
                    Ok(output)
                }
            }
            Err(e) => {
                // Note: Error counting is not currently tracked in this simplified approach
                self.update_stats(&DBOutput::StatementComplete(0), false);
                Err(e)
            }
        }
    }
}

/// Configure environment for INFO level logging before initializing logger
fn setup_test_logging() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // Set environment variables to configure the logger for INFO level
        unsafe {
            std::env::set_var("RUST_LOG", "info");
        }
        // Initialize the existing logger which will pick up our environment configuration
        logger::initialize_logger();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqllogictest::Runner;
    use std::fs;

    struct TestSuiteStats {
        name: String,
        start_time: Instant,
        stats: TestStats,
    }

    impl TestSuiteStats {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                start_time: Instant::now(),
                stats: TestStats::new(),
            }
        }

        fn finish(&mut self) {
            self.stats.duration = self.start_time.elapsed();
            println!("\nTest Suite: {}", self.name);
            self.stats.print_summary();
        }
    }

    fn run_test_file(path: &Path) -> Result<TestStats, Box<dyn Error>> {
        let start_time = Instant::now();
        
        println!("  Running {}", path.display());
        
        // Use the simple runner approach 
        let config = generate_temp_db_config();
        let config_clone = config.clone();
        
        // Create shared stats that will be used by all DB instances
        let shared_stats = Arc::new(Mutex::new(TestStats::new()));
        let shared_stats_clone = shared_stats.clone();
        
        let mut runner = Runner::new(move || {
            let config = generate_temp_db_config(); // Generate fresh config for each connection
            let instance = match DBInstance::new(config.clone()) {
                Ok(instance) => instance,
                Err(e) => return std::future::ready(Err(e)),
            };
            let mut db = match TKDBTest::new_with_config(config) {
                Ok(db) => db,
                Err(e) => return std::future::ready(Err(DBError::SqlError(format!("Failed to create test DB: {}", e)))),
            };
            // Replace the DB's stats with our shared stats
            db.stats = shared_stats_clone.clone();
            std::future::ready(Ok(db))
        });
        
        let result = runner.run_file(path);
        
        // Clean up
        cleanup_temp_files(&config_clone);
        
        // Create stats for the file that was attempted to be run
        let mut stats = TestStats::new();
        stats.files_run = 1; // We attempted to run one file
        stats.duration = start_time.elapsed();
        
        // Get the statistics from the shared stats
        let shared_stats = shared_stats.lock().unwrap();
        stats.statements_run = shared_stats.statements_run;
        stats.queries_run = shared_stats.queries_run;
        stats.errors_caught = shared_stats.errors_caught;
        stats.successful_statements = shared_stats.successful_statements;
        stats.failed_statements = shared_stats.failed_statements;
        stats.successful_queries = shared_stats.successful_queries;
        stats.failed_queries = shared_stats.failed_queries;
        
        match result {
            Ok(_) => {
                // Test file ran successfully (all tests passed)
                Ok(stats)
            }
            Err(e) => {
                // Test file ran but some tests failed - this is still a "file execution"
                // but we need to propagate the error for reporting
                Err(e.into())
            }
        }
    }

    fn run_test_directory(
        dir: &Path,
        suite_stats: &mut TestSuiteStats,
    ) -> Result<(), Box<dyn Error>> {
        let entries = fs::read_dir(dir)?;
        let mut any_failures = false;
        let mut first_error: Option<Box<dyn Error>> = None;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().map_or(false, |ext| ext == "slt") {
                match run_test_file(&path) {
                    Ok(file_stats) => {
                        // Test file succeeded, add to statistics
                        suite_stats.stats.files_run += file_stats.files_run;
                        suite_stats.stats.statements_run += file_stats.statements_run;
                        suite_stats.stats.queries_run += file_stats.queries_run;
                        suite_stats.stats.errors_caught += file_stats.errors_caught;
                        suite_stats.stats.successful_statements += file_stats.successful_statements;
                        suite_stats.stats.failed_statements += file_stats.failed_statements;
                        suite_stats.stats.successful_queries += file_stats.successful_queries;
                        suite_stats.stats.failed_queries += file_stats.failed_queries;
                        suite_stats.stats.duration += file_stats.duration;
                    }
                    Err(e) => {
                        // Test file failed, but we still count it as executed
                        // We create a basic stats entry with files_run = 1
                        suite_stats.stats.files_run += 1;
                        // Duration is already tracked in the failed run, but we don't have access to it
                        // so we'll set a minimal duration
                        suite_stats.stats.duration += Duration::from_millis(1);
                        
                        any_failures = true;
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                }
            } else if path.is_dir() {
                if let Err(e) = run_test_directory(&path, suite_stats) {
                    any_failures = true;
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        // Return error if any tests failed, but after collecting all statistics
        if any_failures {
            if let Some(error) = first_error {
                return Err(error);
            }
        }

        Ok(())
    }

    #[test]
    fn run_ddl_tests() -> Result<(), Box<dyn Error>> {
        setup_test_logging();
        let mut stats = TestSuiteStats::new("DDL Tests");
        let result = run_test_directory(Path::new("tests/sql/ddl"), &mut stats);
        stats.finish();
        cleanup_temp_directory();
        result
    }

    #[test]
    fn run_dml_tests() -> Result<(), Box<dyn Error>> {
        setup_test_logging();
        let mut stats = TestSuiteStats::new("DML Tests");
        let result = run_test_directory(Path::new("tests/sql/dml"), &mut stats);
        stats.finish();
        cleanup_temp_directory();
        result
    }

    #[test]
    fn run_query_tests() -> Result<(), Box<dyn Error>> {
        setup_test_logging();
        let mut stats = TestSuiteStats::new("Query Tests");
        let result = run_test_directory(Path::new("tests/sql/queries"), &mut stats);
        stats.finish();
        cleanup_temp_directory();
        result
    }

    #[test]
    fn run_all_tests() -> Result<(), Box<dyn Error>> {
        setup_test_logging();
        let test_categories = [
            "ddl",
            "dml",
            "queries",
            "transactions",
            "indexes",
            "constraints",
            "functions",
        ];

        let mut total_stats = TestStats::new();
        let start_time = Instant::now();

        for category in test_categories.iter() {
            let dir = PathBuf::from("tests/sql").join(category);
            if dir.exists() {
                let mut suite_stats = TestSuiteStats::new(&format!("{} Tests", category));
                println!("\n{}Running {} tests...{}", BLUE, category, RESET);
                run_test_directory(&dir, &mut suite_stats)?;

                total_stats.files_run += suite_stats.stats.files_run;
                total_stats.statements_run += suite_stats.stats.statements_run;
                total_stats.queries_run += suite_stats.stats.queries_run;
                total_stats.errors_caught += suite_stats.stats.errors_caught;
                total_stats.successful_statements += suite_stats.stats.successful_statements;
                total_stats.successful_queries += suite_stats.stats.successful_queries;
                total_stats.failed_statements += suite_stats.stats.failed_statements;
                total_stats.failed_queries += suite_stats.stats.failed_queries;

                suite_stats.finish();
            }
        }

        total_stats.duration = start_time.elapsed();
        println!("\n{}Overall Test Results:{}", BLUE, RESET);
        println!("====================");
        total_stats.print_summary();

        // Clean up all temporary files
        cleanup_temp_directory();

        Ok(())
    }

    fn run_test_suite(suite_name: &str) -> Result<TestStats, Box<dyn Error>> {
        let dir = PathBuf::from("tests/sql").join(suite_name);
        if !dir.exists() {
            return Err(format!("Test suite '{}' not found", suite_name).into());
        }

        let mut suite_stats = TestSuiteStats::new(&format!("{} Suite", suite_name));
        run_test_directory(&dir, &mut suite_stats)?;
        suite_stats.finish();
        Ok(suite_stats.stats)
    }

    #[test]
    fn run_data_modification_suite() -> Result<(), Box<dyn Error>> {
        setup_test_logging();
        let start_time = Instant::now();
        let mut total_stats = TestStats::new();

        println!("\n{}Running Data Modification Test Suite{}", BLUE, RESET);
        println!("===================================");

        // Run both DDL and DML tests together
        let ddl_stats = run_test_suite("ddl")?;
        let dml_stats = run_test_suite("dml")?;

        total_stats.files_run = ddl_stats.files_run + dml_stats.files_run;
        total_stats.statements_run = ddl_stats.statements_run + dml_stats.statements_run;
        total_stats.queries_run = ddl_stats.queries_run + dml_stats.queries_run;
        total_stats.errors_caught = ddl_stats.errors_caught + dml_stats.errors_caught;
        total_stats.successful_statements =
            ddl_stats.successful_statements + dml_stats.successful_statements;
        total_stats.successful_queries =
            ddl_stats.successful_queries + dml_stats.successful_queries;
        total_stats.failed_statements = ddl_stats.failed_statements + dml_stats.failed_statements;
        total_stats.failed_queries = ddl_stats.failed_queries + dml_stats.failed_queries;
        total_stats.duration = start_time.elapsed();

        println!("\n{}Combined Data Modification Results:{}", BLUE, RESET);
        println!("=================================");
        total_stats.print_summary();

        // Clean up all temporary files
        cleanup_temp_directory();

        Ok(())
    }

    #[test]
    fn run_query_optimization_suite() -> Result<(), Box<dyn Error>> {
        setup_test_logging();
        let start_time = Instant::now();
        let mut total_stats = TestStats::new();

        println!("\n{}Running Query Optimization Test Suite{}", BLUE, RESET);
        println!("===================================");

        // Run queries and indexes tests together
        let query_stats = run_test_suite("queries")?;
        let index_stats = run_test_suite("indexes")?;

        total_stats.files_run = query_stats.files_run + index_stats.files_run;
        total_stats.statements_run = query_stats.statements_run + index_stats.statements_run;
        total_stats.queries_run = query_stats.queries_run + index_stats.queries_run;
        total_stats.errors_caught = query_stats.errors_caught + index_stats.errors_caught;
        total_stats.successful_statements =
            query_stats.successful_statements + index_stats.successful_statements;
        total_stats.successful_queries =
            query_stats.successful_queries + index_stats.successful_queries;
        total_stats.failed_statements =
            query_stats.failed_statements + index_stats.failed_statements;
        total_stats.failed_queries = query_stats.failed_queries + index_stats.failed_queries;
        total_stats.duration = start_time.elapsed();

        println!("\n{}Combined Query Optimization Results:{}", BLUE, RESET);
        println!("==================================");
        total_stats.print_summary();

        // Clean up all temporary files
        cleanup_temp_directory();

        Ok(())
    }
}
