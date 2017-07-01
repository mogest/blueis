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

#[cfg(test)]
mod tests {
    use super::parse_command;
    use super::rusqlite::Connection;
    use super::resp::Value;
    use std::sync::{Arc, Mutex};

    fn conn() -> Arc<Mutex<Connection>> {
        let connection = Connection::open_in_memory().unwrap();
        Arc::new(Mutex::new(connection))
    }

    #[test]
    fn converts_a_valid_value() {
        let cm = conn();
        let value = Value::Array(vec![Value::String("COMMAND".to_string()), Value::String("argument".to_string())]);
        let result = parse_command(&value, &cm).unwrap();

        assert_eq!(result.name, "COMMAND");
        assert_eq!(result.arguments, vec!["argument"]);
    }

    #[test]
    fn rejects_a_value_that_is_not_an_array() {
        let cm = conn();
        let value = Value::String("COMMAND".to_string());
        assert!(parse_command(&value, &cm).is_err());
    }

    #[test]
    fn rejects_a_value_has_non_string_values_in_the_array() {
        let cm = conn();
        let value = Value::Array(vec![Value::String("COMMAND".to_string()), Value::Integer(2)]);
        assert!(parse_command(&value, &cm).is_err());
    }
}
