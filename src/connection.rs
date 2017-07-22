extern crate resp;
extern crate rusqlite;
extern crate time;
extern crate libc;

use std::io::{Write, BufReader, BufWriter};
use std::net::{TcpStream};
use std::sync::{Arc, Mutex, Condvar};
use std::os::unix::io::AsRawFd;
use self::resp::{Decoder, Value};

use commands;
use parser;
use monitor;

pub struct Connection {
    sqlite_connection_mutex: Arc<Mutex<rusqlite::Connection>>,
    monitor: monitor::Monitor,
    push_notification: Arc<(Mutex<bool>, Condvar)>,
    stream: Option<TcpStream>,
}

pub trait Connectionable {
    fn get_push_notification(&self) -> Arc<(Mutex<bool>, Condvar)>;
    fn get_sqlite_connection_mutex(&self) -> &Arc<Mutex<rusqlite::Connection>>;
    fn is_stream_alive(&self) -> bool;
    fn send_to_command_log(&self, command: String);
}

impl Connectionable for Connection {
    fn get_push_notification(&self) -> Arc<(Mutex<bool>, Condvar)> { self.push_notification.clone() }
    fn get_sqlite_connection_mutex(&self) -> &Arc<Mutex<rusqlite::Connection>> { &self.sqlite_connection_mutex }

    fn is_stream_alive(&self) -> bool {
        let fd = self.borrow_stream().as_raw_fd();

        unsafe {
            let mut pollfd = libc::pollfd { fd: fd, events: libc::POLLIN, revents: 0 };
            libc::poll(&mut pollfd, 1, 0);

            pollfd.revents & libc::POLLHUP == 0
        }
    }

    fn send_to_command_log(&self, command: String) {
        self.monitor.send(command);
    }
}

impl Connection {
    pub fn new(sqlite_connection_mutex: Arc<Mutex<rusqlite::Connection>>, monitor: monitor::Monitor, push_notification: Arc<(Mutex<bool>, Condvar)>) -> Connection {
        Connection {
            sqlite_connection_mutex: sqlite_connection_mutex,
            monitor: monitor,
            push_notification: push_notification,
            stream: None,
        }
    }

    fn borrow_stream(&self) -> &TcpStream {
        match self.stream {
            Some(ref stream) => stream,
            None => panic!()
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
                    if writer.write(&result.encode()).is_err() { break; }
                    if writer.flush().is_err() { break; }

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
       let listener = self.monitor.listen();

       loop {
           match listener.recv() {
               Some(data) => {
                   let value = Value::String(data);
                   if writer.write(&value.encode()).is_err() { break; }
                   if writer.flush().is_err() { break; }
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
                    connection: self as &Connectionable,
                };

                command.execute()
            }

            Err(error) => (Value::Error(format!("ERR {}", error)), commands::Action::Continue)
        }
    }
}
