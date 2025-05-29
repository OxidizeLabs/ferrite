use sqllogictest::{DBOutput, DefaultColumnType, DB};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tkdb::common::db_instance::{DBConfig, DBInstance};
use tkdb::common::exception::DBError;
use tkdb::common::result_writer::ResultWriter;
use tkdb::concurrency::transaction::IsolationLevel;
use tkdb::types_db::value::Value;
use tkdb::catalog::schema::Schema;

// Add color constants at the top of the file
const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const BLUE: &str = "\x1b[34m";
const RESET: &str = "\x1b[0m";

#[derive(Default)]
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
            (self.successful_statements as f32 / self.statements_run as f32 * 100.0).max(0.0),
            RESET
        );
        println!(
            "  {}Failed:           {} ({:.1}%){}",
            RED,
            self.failed_statements,
            (self.failed_statements as f32 / self.statements_run as f32 * 100.0).max(0.0),
            RESET
        );
        println!("Queries executed:   {}", self.queries_run);
        println!(
            "  {}Success:          {} ({:.1}%){}",
            GREEN,
            self.successful_queries,
            (self.successful_queries as f32 / self.queries_run as f32 * 100.0).max(0.0),
            RESET
        );
        println!(
            "  {}Failed:           {} ({:.1}%){}",
            RED,
            self.failed_queries,
            (self.failed_queries as f32 / self.queries_run as f32 * 100.0).max(0.0),
            RESET
        );
        println!(
            "{}Errors validated:   {}{}",
            YELLOW, self.errors_caught, RESET
        );
        println!("Total duration:     {:.2?}", self.duration);
        println!(
            "Average time/file:  {:.2?}",
            self.duration.div_f32(self.files_run as f32)
        );
        println!("-------------\n");
    }
}

pub struct TKDBTest {
    instance: DBInstance,
    stats: TestStats,
}

impl TKDBTest {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        let config = DBConfig::default();
        let instance = DBInstance::new(config)?;
        Ok(Self {
            instance,
            stats: TestStats::new(),
        })
    }

    fn update_stats(&mut self, output: &DBOutput<DefaultColumnType>, success: bool) {
        match output {
            DBOutput::StatementComplete(_) => {
                self.stats.statements_run += 1;
                if success {
                    self.stats.successful_statements += 1;
                } else {
                    self.stats.failed_statements += 1;
                }
            }
            DBOutput::Rows { .. } => {
                self.stats.queries_run += 1;
                if success {
                    self.stats.successful_queries += 1;
                } else {
                    self.stats.failed_queries += 1;
                }
            }
            _ => (), // Handle any future variants
        }
    }

    fn get_stats(&self) -> &TestStats {
        &self.stats
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
                self.stats.errors_caught += 1;
                self.update_stats(&DBOutput::StatementComplete(0), false);
                Err(e)
            }
        }
    }
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
        let config = DBConfig::default();
        let start_time = Instant::now();

        let db = TKDBTest::new()?;
        let mut runner = Runner::new(move || {
            let instance = match DBInstance::new(config.clone()) {
                Ok(instance) => instance,
                Err(e) => return std::future::ready(Err(e)),
            };
            std::future::ready(Ok(TKDBTest {
                instance,
                stats: TestStats::new(),
            }))
        });

        println!("  Running {}", path.display());
        runner.run_file(path)?;

        let mut stats = TestStats::new();
        stats.files_run = 1;
        stats.duration = start_time.elapsed();
        Ok(stats)
    }

    fn run_test_directory(
        dir: &Path,
        suite_stats: &mut TestSuiteStats,
    ) -> Result<(), Box<dyn Error>> {
        let entries = fs::read_dir(dir)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().map_or(false, |ext| ext == "slt") {
                let file_stats = run_test_file(&path)?;
                suite_stats.stats.files_run += file_stats.files_run;
                suite_stats.stats.statements_run += file_stats.statements_run;
                suite_stats.stats.queries_run += file_stats.queries_run;
                suite_stats.stats.errors_caught += file_stats.errors_caught;
            }
        }

        Ok(())
    }

    #[test]
    fn run_ddl_tests() -> Result<(), Box<dyn Error>> {
        let mut stats = TestSuiteStats::new("DDL Tests");
        run_test_directory(Path::new("tests/sql/ddl"), &mut stats)?;
        stats.finish();
        Ok(())
    }

    #[test]
    fn run_dml_tests() -> Result<(), Box<dyn Error>> {
        let mut stats = TestSuiteStats::new("DML Tests");
        run_test_directory(Path::new("tests/sql/dml"), &mut stats)?;
        stats.finish();
        Ok(())
    }

    #[test]
    fn run_query_tests() -> Result<(), Box<dyn Error>> {
        let mut stats = TestSuiteStats::new("Query Tests");
        run_test_directory(Path::new("tests/sql/queries"), &mut stats)?;
        stats.finish();
        Ok(())
    }

    #[test]
    fn run_all_tests() -> Result<(), Box<dyn Error>> {
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

        Ok(())
    }

    #[test]
    fn run_query_optimization_suite() -> Result<(), Box<dyn Error>> {
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

        Ok(())
    }
}
