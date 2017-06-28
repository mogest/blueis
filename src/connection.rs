extern crate resp;
extern crate rusqlite;

use std::io::{Write, BufReader, BufWriter};
use std::net::{TcpStream};
use self::resp::{Decoder};
use self::rusqlite::Connection;
use std::sync::{Arc, Mutex};
use commands;

pub fn handle_connection(stream: TcpStream, connection_mutex: Arc<Mutex<Connection>>) {
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let mut decoder = Decoder::new(reader);

    loop {
        if let Ok(value) = decoder.decode() {
            let (result, hangup) = commands::handle_input(value, &connection_mutex);
            writer.write(&result.encode()).unwrap();
            writer.flush().unwrap();

            if hangup { break; }
        }
        else {
            print!("bye!\n");
            break;
        }
    }
}
