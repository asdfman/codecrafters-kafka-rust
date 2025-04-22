#![allow(unused_imports)]
use std::net::TcpListener;

use anyhow::{Context, Result};
use codecrafters_kafka::server;

fn main() -> Result<()> {
    let server = server::Server::new();
    server.run().context("Server error")?;
    Ok(())
}
