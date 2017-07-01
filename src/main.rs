mod connection;
mod commands;
mod parser;
extern crate rusqlite;

use std::env;
use std::io::{self, Write};
use std::net::TcpListener;
use std::thread;
use std::sync::{Arc, Mutex};
use self::rusqlite::Connection;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        writeln!(io::stderr(), "usage: blueis host:port database.sqlite3").unwrap();
        std::process::exit(1);
    }

    let listener = TcpListener::bind(args[1].clone()).unwrap();

    let connection = Connection::open(args[2].clone()).unwrap();

    connection.execute("CREATE TABLE list_items (id integer primary key autoincrement, key string, value blob, position integer)", &[]).ok();
    connection.execute("CREATE INDEX list_items_key ON list_items(key, position)", &[]).ok();

    let connection_mutex = Arc::new(Mutex::new(connection));

    for stream in listener.incoming() {
        match stream {
            Err(_) => {}
            Ok(stream) => {
                let connection_mutex = connection_mutex.clone();
                thread::spawn(move || connection::handle_connection(stream, connection_mutex));
            }
        }
    }
}
