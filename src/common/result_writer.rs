use prettytable::{Cell, Row, Table, format};
use colored::Colorize;
use crate::types_db::value::Value;

/// Trait for writing query results in a tabular format
pub trait ResultWriter {
    fn write_schema_header(&mut self, headers: Vec<String>);
    fn write_row(&mut self, values: Vec<Value>);
    fn write_message(&mut self, message: &str);
}

pub struct CliResultWriter {
    table: Option<Table>,
    headers: Vec<String>,
}

impl Default for CliResultWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl CliResultWriter {
    pub fn new() -> Self {
        Self {
            table: None,
            headers: Vec::new(),
        }
    }

    fn ensure_table(&mut self) {
        if self.table.is_none() {
            let mut table = Table::new();
            table.set_format(*format::consts::FORMAT_BOX_CHARS);
            
            // Add headers if they exist
            if !self.headers.is_empty() {
                table.set_titles(Row::new(
                    self.headers
                        .iter()
                        .map(|h| Cell::new(&h.bold().to_string()))
                        .collect()
                ));
            }
            
            self.table = Some(table);
        }
    }
}

impl ResultWriter for CliResultWriter {
    fn write_schema_header(&mut self, headers: Vec<String>) {
        self.headers = headers;
        self.table = None; // Reset table to create new one with headers
        self.ensure_table();
    }

    fn write_row(&mut self, values: Vec<Value>) {
        self.ensure_table();
        
        if let Some(table) = &mut self.table {
            let row = Row::new(
                values
                    .into_iter()
                    .map(|v| Cell::new(&v.to_string()))
                    .collect()
            );
            table.add_row(row);
        }
    }

    fn write_message(&mut self, message: &str) {
        // Flush any existing table
        if let Some(table) = self.table.take() {
            table.printstd();
            println!(); // Add spacing
        }
        
        println!("{}", message);
    }
}

impl Drop for CliResultWriter {
    fn drop(&mut self) {
        // Print any remaining table
        if let Some(table) = self.table.take() {
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
        writer.write_schema_header(vec![
            "Setting".to_string(),
            "Value".to_string()
        ]);

        // Write rows
        writer.write_row(vec![
            Value::from("Database File"),
            Value::from("default_db.db")
        ]);
        writer.write_row(vec![
            Value::from("Buffer Pool Size"),
            Value::from(1024)
        ]);
    }

    #[test]
    fn test_message_writing() {
        let mut writer = CliResultWriter::new();
        writer.write_message("Test message");
    }
}
