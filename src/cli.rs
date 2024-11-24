use crate::common::db_instance::{DBConfig, DBInstance, DiskManagerType};
use anyhow::Result as AnyResult;
use sqlparser::ast::Table;
use std::io::{stdin, stdout, Write};
use std::path::PathBuf;

struct TableWriter {
    tables: Vec<Table>,
}

impl TableWriter {
    fn new() -> Self {
        Self { tables: Vec::new() }
    }
}

#[derive(Debug)]
enum TransactionState {
    Running,
    // Other states would go here
}

fn main() -> AnyResult<()> {
    let file_config = DBConfig {
        disk_manager_type: DiskManagerType::File {
            path: PathBuf::from(".")
        },
        enable_logging: true,
        ..Default::default()
    };
    let mut DB = DBInstance::new(file_config);
    let default_prompt = "DB> ";
    let emoji_prompt = "🛁> "; // the bathtub emoji

    let args: Vec<String> = std::env::args().collect();
    let use_emoji_prompt = args.iter().any(|arg| arg == "--emoji-prompt");
    let disable_tty = args.iter().any(|arg| arg == "--disable-tty");

    println!("Welcome to the DB shell! Type \\help to learn more.\n");

    let prompt = if use_emoji_prompt { emoji_prompt } else { default_prompt };
    let mut rl = Editor::<()>::new()?;
    rl.set_max_history_size(1024)?;

    loop {
        let mut query = String::new();
        let mut first_line = true;

        loop {
            let context_prompt = if let Some(txn) = DB.current_managed_txn() {
                match txn.get_transaction_state() {
                    TransactionState::Running => {
                        format!("txn{}> ", txn.get_transaction_id_human_readable())
                    }
                    state => {
                        format!(
                            "txn{} ({:?})> ",
                            txn.get_transaction_id_human_readable(),
                            state
                        )
                    }
                }
            } else {
                prompt.to_string()
            };

            let line_prompt = if first_line {
                context_prompt
            } else {
                "... ".to_string()
            };

            let line = if !disable_tty {
                match rl.readline(&line_prompt) {
                    Ok(line) => line,
                    Err(_) => return Ok(()),
                }
            } else {
                print!("{}", line_prompt);
                stdout().flush()?;
                let mut line = String::new();
                if stdin().read_line(&mut line)? == 0 {
                    return Ok(());
                }
                line
            };

            query.push_str(&line);
            if line.trim_end().ends_with(';') || line.trim_start().starts_with('\\') {
                break;
            }
            query.push(if disable_tty { '\n' } else { ' ' });
            first_line = false;
        }

        if !disable_tty {
            rl.add_history_entry(&query)?;
        }

        match execute_query(&mut DB, &query) {
            Ok(()) => (),
            Err(e) => eprintln!("{}", e),
        }
    }
}

fn execute_query(DB: &mut DBInstance, query: &str) -> AnyResult<()> {
    let mut writer = TableWriter::new();
    DB.execute_sql(query, &mut writer)?;

    for table in writer.tables {
        println!("{}", table);
        stdout().flush()?;
    }
    Ok(())
}