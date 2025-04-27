pub struct Server {}
use anyhow::{Context, Result};
use bytes::Bytes;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use crate::handler::process_request;

impl Server {
    pub fn new() -> Self {
        Self {}
    }
    pub fn run(self) -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

        for stream in listener.incoming() {
            match stream {
                Ok(_stream) => {
                    thread::spawn(|| {
                        let _ = handle_connection(_stream);
                    });
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
    while let Ok(length) = stream.read(&mut buffer) {
        let request_bytes = Bytes::copy_from_slice(&buffer[..length]);
        if request_bytes.is_empty() {
            break;
        }
        let response = process_request(request_bytes)?;
        stream
            .write_all(&response)
            .context("Failed to write response")?;
        stream.flush()?;
        thread::sleep(Duration::from_millis(1000));
    }
    Ok(())
}
