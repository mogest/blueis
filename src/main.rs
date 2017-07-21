extern crate rusqlite;

mod connection;
mod commands;
mod parser;
mod monitor;

use std::env;
use std::io::{self, Write};
use std::net::TcpListener;
use std::thread;
use std::sync::{Arc, Mutex, Condvar};

const DATABASE_VERSION: &'static str = "1";

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        writeln!(io::stderr(), "usage: blueis host:port database.sqlite3").unwrap();
        std::process::exit(1);
    }

    let listener = TcpListener::bind(args[1].clone()).unwrap();

    let connection = rusqlite::Connection::open(args[2].clone()).unwrap();

    set_up_tables(&connection);

    let connection_mutex = Arc::new(Mutex::new(connection));

    let push_notification = Arc::new((Mutex::new(false), Condvar::new()));

    let monitor = monitor::Monitor::new();

    println!("blueis listening at {}", args[1]);

    for stream in listener.incoming() {
        match stream {
            Err(_) => {}
            Ok(stream) => {
                let connection_mutex = connection_mutex.clone();
                let local_push_notification = push_notification.clone();
                let local_monitor = monitor.clone();

                thread::spawn(move || {
                    connection::Connection::new(
                        connection_mutex,
                        local_monitor,
                        local_push_notification
                    ).run(stream);
                });
            }
        }
    }
}

fn set_up_tables(connection: &rusqlite::Connection) {
    connection.execute("CREATE TABLE blueis (id integer primary key autoincrement, key string, value blob)", &[]).ok();
    connection.execute("CREATE UNIQUE INDEX blueis_key_index ON blueis(key)", &[]).ok();

    match connection.prepare("SELECT value FROM blueis WHERE key = 'version'").unwrap().query_row(&[], |row| row.get(0)) as Result<String, _> {
        Ok(value) => {
            if value.as_str() != DATABASE_VERSION {
                panic!("the database supplied has been used on a later version of blueis, and therefore is incompatible with this version");
            }
        }

        Err(rusqlite::Error::QueryReturnedNoRows) => {
            connection.execute("INSERT INTO blueis (key, value) VALUES ('version', '1')", &[]).unwrap();
        }

        Err(e) => panic!(e)
    }

    connection.execute("CREATE TABLE list_items (id integer primary key autoincrement, key blob, value blob, position integer)", &[]).ok();
    connection.execute("CREATE INDEX list_items_key ON list_items(key, position)", &[]).ok();
}
