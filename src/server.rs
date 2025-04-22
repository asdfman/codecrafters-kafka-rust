pub struct Server {}
use anyhow::{Context, Result};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use crate::protocol::Response;

impl Server {
    pub fn new() -> Self {
        Self {}
    }
    pub fn run(self) -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

        for stream in listener.incoming() {
            match stream {
                Ok(_stream) => {
                    println!("accepted new connection");
                    handle_connection(_stream)?;
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
        Ok(())
    }
}

fn handle_connection(mut stream: TcpStream) -> Result<()> {
    println!("accepted new connection");
    let mut buffer = [0u8; 1024];
    if let Ok(length) = stream.read(&mut buffer) {
        let response = Response::default();
        let bytes = response.get_bytes();
        stream
            .write_all(&bytes)
            .context("Failed to write response")?;
        stream.flush()?;
    }
    Ok(())
}
