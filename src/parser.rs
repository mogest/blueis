extern crate resp;
extern crate rusqlite;

use self::resp::{Value};
use self::rusqlite::{Connection};
use std::sync::{Arc, Mutex};

use commands::Command;

pub fn parse_command<'a>(value: &'a Value, connection_mutex: &'a Arc<Mutex<Connection>>) -> Result<Command<'a>, &'a str> {
    if let &Value::Array(ref array) = value {
        parse_command_array(array, connection_mutex)
    }
    else {
        Err("expected array")
    }
}

fn parse_command_array<'a>(array: &'a Vec<Value>, connection_mutex: &'a Arc<Mutex<Connection>>) -> Result<Command<'a>, &'a str> {
    let iter = array.iter().map(|value|
        match *value {
            Value::String(ref string) | Value::Bulk(ref string) => Ok(string.as_str()),
            _ => Err("all arguments should be strings")
        }
    );

    let strings = iter.collect::<Result<Vec<&str>, &'static str>>()?;
    let (head, tail) = strings.split_at(1);

    Ok(Command {
        name: head[0],
        arguments: tail.to_vec(),
        connection_mutex: connection_mutex
    })
}
