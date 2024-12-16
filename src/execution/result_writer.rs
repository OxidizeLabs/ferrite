use colored::Colorize;
use crate::common::db_instance::ResultWriter;
use std::collections::VecDeque;

pub struct CliResultWriter {
    table_started: bool,
    current_row: Vec<String>,
    column_widths: Vec<usize>,
    rows: VecDeque<Vec<String>>,
    is_header: bool,
}

impl Default for CliResultWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl CliResultWriter {
    pub fn new() -> Self {
        Self {
            table_started: false,
            current_row: Vec::new(),
            column_widths: Vec::new(),
            rows: VecDeque::new(),
            is_header: false,
        }
    }

    fn update_column_width(&mut self, column_idx: usize, content: &str) {
        while self.column_widths.len() <= column_idx {
            self.column_widths.push(0);
        }
        self.column_widths[column_idx] = self.column_widths[column_idx].max(content.len());
    }

    fn flush_current_row(&mut self) {
        if !self.current_row.is_empty() {
            self.rows.push_back(std::mem::take(&mut self.current_row));
        }
    }

    fn print_table(&mut self) {
        // Print each row with proper padding
        for row in self.rows.drain(..) {
            for (idx, cell) in row.iter().enumerate() {
                let width = self.column_widths.get(idx).copied().unwrap_or(15);
                if idx > 0 {
                    print!("  "); // Add spacing between columns
                }
                print!("{:<width$}", cell, width = width);
            }
            println!();
        }
    }
}

impl ResultWriter for CliResultWriter {
    fn begin_table(&mut self, _bordered: bool) {
        self.table_started = true;
        self.column_widths.clear();
        self.rows.clear();
        self.is_header = false;
    }

    fn end_table(&mut self) {
        if self.table_started {
            self.flush_current_row();
            self.print_table();
            println!();
            self.table_started = false;
        }
    }

    fn begin_header(&mut self) {
        self.is_header = true;
    }

    fn end_header(&mut self) {
        self.flush_current_row();
        self.is_header = false;
    }

    fn begin_row(&mut self) {
        self.current_row.clear();
    }

    fn end_row(&mut self) {
        self.flush_current_row();
    }

    fn write_cell(&mut self, content: &str) {
        let column_idx = self.current_row.len();
        self.update_column_width(column_idx, content);
        self.current_row.push(content.to_string());
    }

    fn write_header_cell(&mut self, content: &str) {
        let column_idx = self.current_row.len();
        self.update_column_width(column_idx, content);
        self.current_row.push(content.bold().to_string());
    }

    fn one_cell(&mut self, content: &str) {
        println!("{}", content);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_formatting() {
        let mut writer = CliResultWriter::new();

        writer.begin_table(true);

        // Write header
        writer.begin_header();
        writer.write_header_cell("Setting");
        writer.write_header_cell("Value");
        writer.end_header();

        // Write rows
        writer.begin_row();
        writer.write_cell("Database File");
        writer.write_cell("default_db.db");
        writer.end_row();

        writer.begin_row();
        writer.write_cell("Buffer Pool Size");
        writer.write_cell("1024");
        writer.end_row();

        writer.end_table();
    }
}