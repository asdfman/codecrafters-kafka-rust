pub struct Server {}
use anyhow::{Context, Result};
use bytes::Bytes;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use crate::protocol::{Request, Response};

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
        let request_bytes = Bytes::copy_from_slice(&buffer[..length]);
        let request = Request::from(request_bytes);
        let response = Response::new(request.correlation_id, 35);
        stream
            .write_all(&Bytes::from(response))
            .context("Failed to write response")?;
        stream.flush()?;
    }
    Ok(())
}
