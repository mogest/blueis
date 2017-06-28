mod connection;
mod commands;
extern crate rusqlite;

use std::net::{TcpListener};
use std::thread;
use std::sync::{Arc, Mutex};
use self::rusqlite::Connection;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:3000").unwrap();

    let connection = Connection::open("database.sqlite3").unwrap();

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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
