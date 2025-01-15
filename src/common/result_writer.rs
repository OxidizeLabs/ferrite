use colored::Colorize;
use std::collections::VecDeque;
use crate::storage::table::tuple::Tuple;

/// Trait for writing query results in a tabular format
pub trait ResultWriter {
    fn begin_table(&mut self, bordered: bool);
    fn end_table(&mut self);
    fn begin_header(&mut self);
    fn end_header(&mut self);
    fn begin_row(&mut self);
    fn end_row(&mut self);
    fn write_cell(&mut self, content: &str);
    fn write_header_cell(&mut self, content: &str);
    fn one_cell(&mut self, content: &str);
}

pub struct CliResultWriter {
    table_started: bool,
    current_row: Vec<String>,
    column_widths: Vec<usize>,
    rows: VecDeque<Vec<String>>,
    is_header: bool,
    bordered: bool,
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
            bordered: false,
        }
    }

    fn update_column_width(&mut self, column_idx: usize, content: &str) {
        while self.column_widths.len() <= column_idx {
            self.column_widths.push(0);
        }
        // For header cells, we need to account for the actual content length, not the length with ANSI codes
        let display_length = strip_ansi_len(content);
        self.column_widths[column_idx] = self.column_widths[column_idx].max(display_length);
    }

    fn flush_current_row(&mut self) {
        if !self.current_row.is_empty() {
            self.rows.push_back(std::mem::take(&mut self.current_row));
        }
    }

    fn print_horizontal_line(&self, style: BorderStyle) {
        if !self.bordered {
            return;
        }

        let (left, cross, right) = match style {
            BorderStyle::Top => ("╔", "╦", "╗"),
            BorderStyle::Header => ("╠", "╬", "╣"),
            BorderStyle::Row => ("╟", "╫", "╢"),
            BorderStyle::Bottom => ("╚", "╩", "╝"),
        };

        print!("{}", left);
        for (idx, width) in self.column_widths.iter().enumerate() {
            print!(
                "{}",
                if style == BorderStyle::Row {
                    "─"
                } else {
                    "═"
                }
                    .repeat(*width)
            );
            if idx < self.column_widths.len() - 1 {
                print!("{}", cross);
            }
        }
        println!("{}", right);
    }

    fn print_row(&self, row: &[String], is_last: bool) {
        if !self.bordered {
            // Simple table without borders
            for (idx, cell) in row.iter().enumerate() {
                let width = self.column_widths.get(idx).copied().unwrap_or(15);
                if idx > 0 {
                    print!("  ");
                }
                print!("{:<width$}", pad_string(cell, width), width = width);
            }
            println!();
            return;
        }

        // Print bordered row
        print!("║");
        for (idx, cell) in row.iter().enumerate() {
            let width = self.column_widths.get(idx).copied().unwrap_or(15);
            print!("{:<width$}", pad_string(cell, width), width = width);
            if idx < row.len() - 1 {
                print!("║");
            }
        }
        println!("║");

        if !is_last {
            self.print_horizontal_line(BorderStyle::Row);
        }
    }

    fn print_table(&mut self) {
        if self.rows.is_empty() {
            return;
        }

        if self.bordered {
            self.print_horizontal_line(BorderStyle::Top);
        }

        let mut rows: Vec<_> = self.rows.drain(..).collect();

        // Print header if present
        if !rows.is_empty() {
            let header = rows.remove(0);
            self.print_row(&header, rows.is_empty());
            if self.bordered && !rows.is_empty() {
                self.print_horizontal_line(BorderStyle::Header);
            }
        }

        // Print remaining rows
        for (idx, row) in rows.iter().enumerate() {
            let is_last = idx == rows.len() - 1;
            self.print_row(row, is_last);
        }

        if self.bordered {
            self.print_horizontal_line(BorderStyle::Bottom);
        }
    }
}

#[derive(PartialEq)]
enum BorderStyle {
    Top,
    Header,
    Row,
    Bottom,
}

// Helper function to strip ANSI codes and get true string length
fn strip_ansi_len(s: &str) -> usize {
    strip_ansi(s).chars().count()
}

// Helper function to strip ANSI codes
fn strip_ansi(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut in_ansi = false;

    for c in s.chars() {
        if c == '\x1B' {
            in_ansi = true;
            continue;
        }
        if in_ansi {
            if c == 'm' {
                in_ansi = false;
            }
            continue;
        }
        result.push(c);
    }
    result
}

// Helper function to pad a string considering ANSI codes
fn pad_string(s: &str, width: usize) -> String {
    let visible_len = strip_ansi_len(s);
    let padding = if visible_len < width {
        " ".repeat(width - visible_len)
    } else {
        String::new()
    };
    format!("{}{}", s, padding)
}

impl ResultWriter for CliResultWriter {
    fn begin_table(&mut self, bordered: bool) {
        self.table_started = true;
        self.bordered = bordered;
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
            self.is_header = false;
        }
    }

    fn begin_header(&mut self) {
        self.is_header = true;
    }

    fn end_header(&mut self) {
        self.flush_current_row();
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
        let bold_content = content.bold().to_string();
        self.update_column_width(column_idx, &bold_content);
        self.current_row.push(bold_content);
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
