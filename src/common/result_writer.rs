//! # Result Writer
//!
//! This module provides abstractions for formatting and outputting query results. It supports
//! multiple output targets through the `ResultWriter` trait, with concrete implementations
//! for CLI (pretty-printed tables) and network (serializable) responses.
//!
//! ## Architecture
//!
//! ```text
//!   ┌──────────────────────────────────────────────────────────────────────────┐
//!   │                        ResultWriter Trait                                │
//!   │                                                                          │
//!   │   write_schema_header(Vec<String>)      Set column headers               │
//!   │   write_row(Vec<Value>)                 Write a data row                 │
//!   │   write_row_with_schema(&Schema)        Write row with type formatting   │
//!   │   write_message(&str)                   Write informational message      │
//!   │                                                                          │
//!   │   Required: Send + Sync                                                  │
//!   └──────────────────────────────────────────────────────────────────────────┘
//!                    │                                │
//!                    │                                │
//!         ┌──────────┴──────────┐          ┌─────────┴──────────┐
//!         │                     │          │                    │
//!         ▼                     ▼          ▼                    ▼
//!   ┌─────────────────────┐  ┌──────────────────────────────────────────┐
//!   │   CliResultWriter   │  │        NetworkResultWriter              │
//!   │                     │  │                                          │
//!   │  Pretty-printed     │  │  Serializable for wire protocol          │
//!   │  terminal output    │  │                                          │
//!   │                     │  │  column_names: Vec<String>               │
//!   │  table: Table       │  │  rows: Vec<Vec<Value>>                   │
//!   │  headers: Vec<Str>  │  │  messages: Vec<String>                   │
//!   │                     │  │                                          │
//!   │  Uses: prettytable  │  │  → into_results() → QueryResults         │
//!   │  Uses: colored      │  │                                          │
//!   └─────────────────────┘  └──────────────────────────────────────────┘
//!           │                                    │
//!           ▼                                    ▼
//!   ┌─────────────────────┐          ┌──────────────────────────────────┐
//!   │  Terminal Output    │          │  QueryResults (bincode → TCP)   │
//!   │                     │          │                                  │
//!   │  ┌───────┬───────┐  │          │  Sent to DatabaseClient          │
//!   │  │ Name  │ Value │  │          │                                  │
//!   │  ├───────┼───────┤  │          └──────────────────────────────────┘
//!   │  │ Alice │   42  │  │
//!   │  │ Bob   │   99  │  │
//!   │  └───────┴───────┘  │
//!   └─────────────────────┘
//! ```
//!
//! ## Data Flow
//!
//! ```text
//!   ExecutionEngine
//!        │
//!        │  executes query
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  Executor produces results                                             │
//!   │                                                                        │
//!   │   1. write_schema_header(["id", "name", "email"])                      │
//!   │   2. write_row([1, "Alice", "alice@example.com"])                      │
//!   │   3. write_row([2, "Bob", "bob@example.com"])                          │
//!   │   4. write_message("2 rows returned")                                  │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │                                    │
//!        │ (CLI mode)                         │ (Server mode)
//!        ▼                                    ▼
//!   ┌────────────────────┐           ┌─────────────────────────────────┐
//!   │  CliResultWriter   │           │  NetworkResultWriter            │
//!   │                    │           │                                 │
//!   │  Builds Table      │           │  Collects data                  │
//!   │  On drop → print   │           │  into_results() → QueryResults  │
//!   └────────────────────┘           └─────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component            | Purpose                                        |
//! |----------------------|------------------------------------------------|
//! | `ResultWriter`       | Trait defining result output interface         |
//! | `CliResultWriter`    | Pretty-printed terminal tables                 |
//! | `NetworkResultWriter`| Serializable results for client-server         |
//! | `QueryResults`       | Final serializable result structure            |
//!
//! ## ResultWriter Methods
//!
//! | Method                  | Description                                  |
//! |-------------------------|----------------------------------------------|
//! | `write_schema_header()` | Set column names for result set              |
//! | `write_row()`           | Add a row of values                          |
//! | `write_row_with_schema()`| Add row with schema-aware formatting        |
//! | `write_message()`       | Write informational/status message           |
//!
//! ## CliResultWriter Features
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  Pretty Table Output                                                    │
//!   │                                                                         │
//!   │   ┌──────────────┬─────────────────────────┐                            │
//!   │   │ Setting      │ Value                   │  ◄── Bold headers          │
//!   │   ├──────────────┼─────────────────────────┤                            │
//!   │   │ Database     │ default_db.db           │                            │
//!   │   │ Buffer Pool  │ 1024                    │                            │
//!   │   └──────────────┴─────────────────────────┘                            │
//!   │                                                                         │
//!   │   Features:                                                             │
//!   │   - Box drawing characters (FORMAT_BOX_CHARS)                           │
//!   │   - Bold column headers (colored crate)                                 │
//!   │   - Schema-aware value formatting                                       │
//!   │   - Auto-print on Drop                                                  │
//!   │   - Thread-safe (Arc<Mutex<>>)                                          │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## NetworkResultWriter Features
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  Network Result Collection                                              │
//!   │                                                                         │
//!   │   column_names: ["id", "name", "email"]                                 │
//!   │   rows: [                                                               │
//!   │     [Int(1), String("Alice"), String("alice@example.com")],             │
//!   │     [Int(2), String("Bob"), String("bob@example.com")],                 │
//!   │   ]                                                                     │
//!   │   messages: ["2 rows returned"]                                         │
//!   │                                                                         │
//!   │   → into_results() → QueryResults { column_names, rows, messages }      │
//!   │   → bincode::encode → TCP → DatabaseClient                              │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::common::result_writer::{ResultWriter, CliResultWriter, NetworkResultWriter};
//! use crate::types_db::value::Value;
//!
//! // CLI usage (embedded mode)
//! fn display_results_cli() {
//!     let mut writer = CliResultWriter::new();
//!
//!     writer.write_schema_header(vec!["Name".to_string(), "Age".to_string()]);
//!     writer.write_row(vec![Value::from("Alice"), Value::from(30)]);
//!     writer.write_row(vec![Value::from("Bob"), Value::from(25)]);
//!     writer.write_message("2 rows returned");
//!
//!     // Table is automatically printed when writer is dropped
//! }
//!
//! // Network usage (server mode)
//! fn build_network_response() -> QueryResults {
//!     let mut writer = NetworkResultWriter::new();
//!
//!     writer.write_schema_header(vec!["Name".to_string(), "Age".to_string()]);
//!     writer.write_row(vec![Value::from("Alice"), Value::from(30)]);
//!     writer.write_row(vec![Value::from("Bob"), Value::from(25)]);
//!
//!     // Convert to serializable QueryResults
//!     writer.into_results()
//! }
//!
//! // Generic function accepting any ResultWriter
//! fn output_results<W: ResultWriter>(writer: &mut W, data: Vec<Vec<Value>>) {
//!     writer.write_schema_header(vec!["Col1".to_string(), "Col2".to_string()]);
//!     for row in data {
//!         writer.write_row(row);
//!     }
//! }
//! ```
//!
//! ## Thread Safety
//!
//! - `ResultWriter` requires `Send + Sync`
//! - `CliResultWriter` uses `Arc<Mutex<>>` for interior mutability
//! - `NetworkResultWriter` is single-threaded (owned by request handler)
//!
//! ## Implementation Notes
//!
//! - **CliResultWriter Drop**: Automatically prints table on drop
//! - **Schema-Aware Formatting**: `write_row_with_schema()` uses column type info
//! - **Message Flushing**: `write_message()` flushes pending table first
//! - **Lazy Table Creation**: Table created on first row write

use crate::catalog::schema::Schema;
use crate::server::QueryResults;
use crate::types_db::value::Value;
use colored::Colorize;
use prettytable::{Cell, Row, Table, format};
use std::sync::{Arc, Mutex};

/// Trait for writing query results in a tabular format.
///
/// Implementations handle different output targets (CLI, network, etc.).
pub trait ResultWriter: Send + Sync {
    fn write_schema_header(&mut self, headers: Vec<String>);
    fn write_row(&mut self, values: Vec<Value>);
    fn write_row_with_schema(&mut self, values: Vec<Value>, schema: &Schema);
    fn write_message(&mut self, message: &str);
}

#[derive(Default)]
pub struct CliResultWriter {
    table: Arc<Mutex<Option<Table>>>,
    headers: Arc<Mutex<Vec<String>>>,
}

// New result writer for network responses
pub struct NetworkResultWriter {
    column_names: Vec<String>,
    rows: Vec<Vec<Value>>,
    messages: Vec<String>,
}

impl NetworkResultWriter {
    pub(crate) fn new() -> Self {
        Self {
            column_names: Vec::new(),
            rows: Vec::new(),
            messages: Vec::new(),
        }
    }

    pub(crate) fn into_results(self) -> QueryResults {
        QueryResults {
            column_names: self.column_names,
            rows: self.rows,
            messages: self.messages,
        }
    }
}

impl CliResultWriter {
    pub fn new() -> Self {
        Self {
            table: Arc::new(Mutex::new(None)),
            headers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn ensure_table(&mut self) {
        if self.table.lock().unwrap().is_none() {
            let mut table = Table::new();
            table.set_format(*format::consts::FORMAT_BOX_CHARS);

            // Add headers if they exist
            if !self.headers.lock().unwrap().is_empty() {
                table.set_titles(Row::new(
                    self.headers
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|h| Cell::new(&h.bold().to_string()))
                        .collect(),
                ));
            }

            *self.table.lock().unwrap() = Some(table);
        }
    }
}

impl ResultWriter for CliResultWriter {
    fn write_schema_header(&mut self, headers: Vec<String>) {
        *self.headers.lock().unwrap() = headers;
        *self.table.lock().unwrap() = None; // Reset table to create new one with headers
        self.ensure_table();
    }

    fn write_row(&mut self, values: Vec<Value>) {
        self.ensure_table();

        if let Some(table) = self.table.lock().unwrap().as_mut() {
            let row = Row::new(
                values
                    .into_iter()
                    .map(|v| Cell::new(&v.to_string()))
                    .collect(),
            );
            table.add_row(row);
        }
    }

    fn write_row_with_schema(&mut self, values: Vec<Value>, schema: &Schema) {
        self.ensure_table();

        if let Some(table) = self.table.lock().unwrap().as_mut() {
            let row = Row::new(
                values
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let formatted_str = if let Some(column) = schema.get_column(i) {
                            v.format_with_column_context(Some(column))
                        } else {
                            v.to_string()
                        };
                        Cell::new(&formatted_str)
                    })
                    .collect(),
            );
            table.add_row(row);
        }
    }

    fn write_message(&mut self, message: &str) {
        // Flush any existing table
        if let Some(table) = self.table.lock().unwrap().take() {
            table.printstd();
            println!(); // Add spacing
        }

        println!("{}", message);
    }
}

impl ResultWriter for NetworkResultWriter {
    fn write_schema_header(&mut self, column_names: Vec<String>) {
        self.column_names = column_names;
    }

    fn write_row(&mut self, values: Vec<Value>) {
        self.rows.push(values);
    }

    fn write_row_with_schema(&mut self, values: Vec<Value>, _schema: &Schema) {
        // For network responses, we still store the original values
        // but the formatting will be handled when converting to strings
        self.rows.push(values);
    }

    fn write_message(&mut self, message: &str) {
        self.messages.push(message.to_string());
    }
}

impl Drop for CliResultWriter {
    fn drop(&mut self) {
        // Print any remaining table
        if let Some(table) = self.table.lock().unwrap().take() {
            table.printstd();
            println!(); // Add spacing
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types_db::value::Value;

    #[test]
    fn test_table_formatting() {
        let mut writer = CliResultWriter::new();

        // Write headers
        writer.write_schema_header(vec!["Setting".to_string(), "Value".to_string()]);

        // Write rows
        writer.write_row(vec![
            Value::from("Database File"),
            Value::from("default_db.db"),
        ]);
        writer.write_row(vec![Value::from("Buffer Pool Size"), Value::from(1024)]);
    }

    #[test]
    fn test_message_writing() {
        let mut writer = CliResultWriter::new();
        writer.write_message("Test message");
    }
}
