use tkdb::cli::CLI;
use tkdb::common::exception::DBError;

fn main() -> Result<(), DBError> {
    // Create and run the CLI
    let mut cli = CLI::new()?;
    cli.run()
}
