extern crate resp;
extern crate rusqlite;
extern crate bus;
extern crate time;

use std::io::{Write, BufReader, BufWriter};
use std::net::{TcpStream};
use self::resp::{Decoder, Value};
use std::sync::{Arc, Mutex, Condvar};
use self::bus::Bus;
use std::sync::mpsc::Sender;

use commands;
use parser;

pub struct Connection {
    pub sqlite_connection_mutex: Arc<Mutex<rusqlite::Connection>>,
    monitor_bus: Arc<Mutex<Bus<String>>>,
    pub command_log_tx: Sender<String>,
    pub push_notification: Arc<(Mutex<bool>, Condvar)>,
}

impl Connection {
    pub fn new(sqlite_connection_mutex: Arc<Mutex<rusqlite::Connection>>, monitor_bus: Arc<Mutex<Bus<String>>>, command_log_tx: Sender<String>, push_notification: Arc<(Mutex<bool>, Condvar)>) -> Connection {
        Connection {
            sqlite_connection_mutex: sqlite_connection_mutex,
            monitor_bus: monitor_bus,
            command_log_tx: command_log_tx,
            push_notification: push_notification,
        }
    }

    pub fn run(&self, stream: TcpStream) {
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let mut decoder = Decoder::with_buf_bulk(reader);

        loop {
            match decoder.decode() {
                Ok(value) => {
                    let (result, action) = self.handle_input(value);
                    writer.write(&result.encode()).unwrap();
                    writer.flush().unwrap();

                    match action {
                        commands::Action::HangUp => break,
                        commands::Action::StartMonitor => {
                            self.run_monitor(writer);
                            break;
                        }
                        _ => {}
                    };
                }

                _ => break
            }
        }
    }

    fn run_monitor(&self, mut writer: BufWriter<&TcpStream>) {
       let mut rx = { self.monitor_bus.lock().unwrap().add_rx() };

       loop {
           match rx.recv() {
               Ok(data) => {
                   let value = Value::String(data);
                   writer.write(&value.encode()).unwrap();
                   writer.flush().unwrap();
               }

               _ => break
           }
       }
    }

    fn handle_input(&self, ref value: Value) -> (Value, commands::Action) {
        match parser::parse_command(value) {
            Ok((name, arguments)) => {
                let mut command = commands::Command {
                    name:       name,
                    arguments:  arguments,
                    connection: &self,
                };

                command.execute()
            }

            Err(error) => (Value::Error(format!("ERR {}", error)), commands::Action::Continue)
        }
    }
}
