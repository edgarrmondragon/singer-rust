use clap::Parser;

/// Singer Summarize
#[derive(Parser, Debug)]
#[command(version)]
pub struct Args {
    /// Config file
    #[clap(short, long)]
    pub config: Option<String>,
}
