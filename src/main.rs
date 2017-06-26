mod connection;
mod commands;

use std::net::{TcpListener};
use std::thread;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:3000").unwrap();

    for stream in listener.incoming() {
        match stream {
            Err(_) => {}
            Ok(stream) => {
                thread::spawn(move || connection::handle_connection(stream));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
