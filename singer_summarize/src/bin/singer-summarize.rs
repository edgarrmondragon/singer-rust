use std::io;

use clap::Parser;
use serde_json::to_string_pretty;

use singer_rust::MessageReader;
use singer_summarize::{cli, StatsReader};

pub fn main() {
    let _args = cli::Args::parse();

    let mut reader = StatsReader::new();
    let buffer = io::BufReader::new(io::stdin());
    reader.process_lines(buffer).expect("valid messages");

    let output = to_string_pretty(&reader.stats).expect("valid counts map");
    println!("{}", output);
}
