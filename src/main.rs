use std::process;

use clap::Parser;

use vtf::cli::commands::Cli;
use vtf::cli::handlers;

fn main() {
    let cli = Cli::parse();

    if let Err(e) = handlers::run(cli.command) {
        eprintln!("error: {e}");
        process::exit(1);
    }
}
