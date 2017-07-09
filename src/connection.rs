extern crate resp;
extern crate rusqlite;
extern crate bus;
extern crate time;
extern crate libc;

use std::io::{Write, BufReader, BufWriter};
use std::net::{TcpStream};
use self::resp::{Decoder, Value};
use std::sync::{Arc, Mutex, Condvar};
use self::bus::Bus;
use std::sync::mpsc::Sender;
use std::os::unix::io::AsRawFd;

use commands;
use parser;

pub struct Connection {
    sqlite_connection_mutex: Arc<Mutex<rusqlite::Connection>>,
    monitor_bus: Arc<Mutex<Bus<String>>>,
    command_log_tx: Sender<String>,
    push_notification: Arc<(Mutex<bool>, Condvar)>,
    stream: Option<TcpStream>,
}

impl Connection {
    pub fn new(sqlite_connection_mutex: Arc<Mutex<rusqlite::Connection>>, monitor_bus: Arc<Mutex<Bus<String>>>, command_log_tx: Sender<String>, push_notification: Arc<(Mutex<bool>, Condvar)>) -> Connection {
        Connection {
            sqlite_connection_mutex: sqlite_connection_mutex,
            monitor_bus: monitor_bus,
            command_log_tx: command_log_tx,
            push_notification: push_notification,
            stream: None,
        }
    }

    pub fn get_command_log_tx(&self) -> &Sender<String> { &self.command_log_tx }
    pub fn get_push_notification(&self) -> Arc<(Mutex<bool>, Condvar)> { self.push_notification.clone() }
    pub fn get_sqlite_connection_mutex(&self) -> &Arc<Mutex<rusqlite::Connection>> { &self.sqlite_connection_mutex }

    fn borrow_stream(&self) -> &TcpStream {
        match self.stream {
            Some(ref stream) => stream,
            None => panic!()
        }
    }

    pub fn is_stream_alive(&self) -> bool {
        let fd = self.borrow_stream().as_raw_fd();

        unsafe {
            let mut pollfd = libc::pollfd { fd: fd, events: libc::POLLIN, revents: 0 };
            libc::poll(&mut pollfd, 1, 0);

            pollfd.revents & libc::POLLHUP == 0
        }
    }

    pub fn run(&mut self, stream: TcpStream) {
        self.stream = Some(stream);

        let stream = self.borrow_stream();
        let reader = BufReader::new(stream);
        let mut writer = BufWriter::new(stream);
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
