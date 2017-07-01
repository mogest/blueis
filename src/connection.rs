extern crate resp;
extern crate rusqlite;

use std::io::{Write, BufReader, BufWriter};
use std::net::{TcpStream};
use self::resp::{Decoder, Value};
use std::sync::{Arc, Mutex};

use commands;
use parser;

pub struct Connection {
    connection_mutex: Arc<Mutex<rusqlite::Connection>>
}

impl Connection {
    pub fn new(connection_mutex: Arc<Mutex<rusqlite::Connection>>) -> Connection {
        Connection {
            connection_mutex: connection_mutex
        }
    }

    pub fn run(&self, stream: TcpStream) {
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let mut decoder = Decoder::new(reader);

        loop {
            match decoder.decode() {
                Ok(value) => {
                    let (result, hangup) = self.handle_input(value);
                    writer.write(&result.encode()).unwrap();
                    writer.flush().unwrap();

                    if hangup { break; }
                }

                _ => break
            }
        }
    }

    fn handle_input(&self, ref value: Value) -> (Value, bool) {
        match parser::parse_command(value) {
            Ok((name, arguments)) => {
                let mut command = commands::Command {
                    name:             name,
                    arguments:        arguments,
                    connection_mutex: &self.connection_mutex
                };

                command.execute()
            }

            Err(error) => (Value::Error(format!("ERR {}", error)), false)
        }
    }
}
