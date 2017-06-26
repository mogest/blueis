extern crate resp;
extern crate rusqlite;

use std::io::{Write, BufReader, BufWriter};
use std::net::{TcpStream};
use self::resp::{Decoder};
use self::rusqlite::Connection;
use commands;

pub fn handle_connection(stream: TcpStream) {
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let mut decoder = Decoder::new(reader);

    let mut connection = Connection::open("database.sqlite3").unwrap();
    connection.execute("CREATE TABLE list_items (id integer primary key autoincrement, key string, value blob, position integer)", &[]);
    connection.execute("CREATE INDEX list_items_key ON list_items(key, position)", &[]);

    loop {
        if let Ok(value) = decoder.decode() {
            let (result, hangup) = commands::handle_input(value, &mut connection);
            writer.write(&result.encode());
            writer.flush();

            if hangup { break; }
        }
        else {
            print!("bye!\n");
            break;
        }
    }
}
