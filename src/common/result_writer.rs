use crate::catalog::schema::Schema;
use crate::server::QueryResults;
use crate::types_db::value::Value;
use colored::Colorize;
use prettytable::{format, Cell, Row, Table};
use std::sync::{Arc, Mutex};

/// Trait for writing query results in a tabular format
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
    fn write_message(&mut self, message: &str) {
        self.messages.push(message.to_string());
    }

    fn write_schema_header(&mut self, column_names: Vec<String>) {
        self.column_names = column_names;
    }

    fn write_row(&mut self, values: Vec<Value>) {
        self.rows.push(values);
    }

    fn write_row_with_schema(&mut self, values: Vec<Value>, schema: &Schema) {
        // For network responses, we still store the original values
        // but the formatting will be handled when converting to strings
        self.rows.push(values);
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
