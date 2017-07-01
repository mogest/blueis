extern crate resp;
extern crate rusqlite;

use std::io::{Write, BufReader, BufWriter};
use std::net::{TcpStream};
use self::resp::{Decoder, Value};
use self::rusqlite::Connection;
use std::sync::{Arc, Mutex};

use parser;

pub fn handle_connection(stream: TcpStream, connection_mutex: Arc<Mutex<Connection>>) {
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let mut decoder = Decoder::new(reader);

    loop {
        match decoder.decode() {
            Ok(value) => {
                let (result, hangup) = handle_input(value, &connection_mutex);
                writer.write(&result.encode()).unwrap();
                writer.flush().unwrap();

                if hangup { break; }
            }

            _ => break
        }
    }
}

fn handle_input(ref value: Value, connection_mutex: &Arc<Mutex<Connection>>) -> (Value, bool) {
    match parser::parse_command(value, connection_mutex) {
        Ok(mut command) => command.handle_command(),
        Err(error)      => (Value::Error(format!("ERR {}", error)), false)
    }
}
