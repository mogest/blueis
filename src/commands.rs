extern crate resp;
extern crate rusqlite;

use self::resp::{Value};
use self::rusqlite::{Connection, Error};
use std::sync::{Arc, Mutex, MutexGuard};

struct Command<'a> {
    name: String,
    arguments: Vec<&'a String>,
    connection_mutex: &'a Arc<Mutex<Connection>>
}

struct CommandSettings {
    name: &'static str,
    argument_count: i32,
    //handler: fn(&'a Command<'a>) -> Result<Value, &'static str>
}

const COMMAND_SETTINGS: [CommandSettings; 5] = [
    CommandSettings { name: "LLEN", argument_count: 1 }, //, handler: Command::llen }
    CommandSettings { name: "LPOP", argument_count: 1 },
    CommandSettings { name: "RPOP", argument_count: 1 },
    CommandSettings { name: "LPUSH", argument_count: -2 },
    CommandSettings { name: "RPUSH", argument_count: -2 },
];

pub fn handle_input(ref value: Value, connection_mutex: &Arc<Mutex<Connection>>) -> (Value, bool) {
    match parse_command(value, connection_mutex) {
        Ok(mut command) => { command.handle_command() }
        Err(error)  => { (Value::Error(format!("ERR {}", error)), false) }
    }
}

fn parse_command<'a>(value: &'a Value, connection_mutex: &'a Arc<Mutex<Connection>>) -> Result<Command<'a>, &'static str> {
    if let &Value::Array(ref array) = value {
        parse_command_array(array, connection_mutex)
    }
    else {
        Err("expected array")
    }
}

fn parse_command_array<'a>(array: &'a Vec<Value>, connection_mutex: &'a Arc<Mutex<Connection>>) -> Result<Command<'a>, &'static str> {
    let iter = array.iter().map(|value|
        match *value {
            Value::String(ref string) | Value::Bulk(ref string) => Ok(string),
            _ => Err("all arguments should be strings")
        }
    );

    let strings = iter.collect::<Result<Vec<&String>, &'static str>>()?;
    let (head, tail) = strings.split_at(1);

    Ok(Command {
        name: head[0].to_uppercase(),
        arguments: tail.to_vec(),
        connection_mutex: connection_mutex
    })
}

impl<'a> Command<'a> {
    fn handle_command(&mut self) -> (Value, bool) {
        print!("command {}\n", self.name);

        match self.name.as_ref() {
            "QUIT" => (Value::String("OK".to_string()), true),
            _      => (self.handle_nonterminal_command(), false)
        }
    }

    fn valid_argument_count(&self, command: &CommandSettings) -> bool {
        (command.argument_count < 0 && self.arguments.len() as i32 >= -command.argument_count) ||
            (command.argument_count >= 0 && self.arguments.len() as i32 == command.argument_count)
    }

    fn handle_nonterminal_command(&mut self) -> Value {
        let all = COMMAND_SETTINGS;
        let settings = all.iter().find(|settings| settings.name == self.name);

        match settings {
            Some(settings) => {
                if self.valid_argument_count(settings) {
                    let result = match settings.name {
                        "LLEN" => self.llen(),
                        "LPUSH" => self.lpush(),
                        "RPUSH" => self.rpush(),
                        "LPOP" => self.lpop(),
                        "RPOP" => self.rpop(),
                        _ => unimplemented!(),
                    };

                    match result {
                        Ok(value)  => value,
                        Err(error) => Value::Error(format!("ERR {}", error))
                    }
                }
                else {
                    Value::Error("ERR wrong number of arguments".to_string())
                }
            }

            None => Value::Error("ERR unsupported".to_string())
        }
    }

    fn llen(&self) -> Result<Value, &'static str> {
        let key = self.arguments[0];

        let connection = self.lock_connection();
        self.count_list_items(&*connection, key)
    }

    fn lpop(&self) -> Result<Value, &'static str> {
        self.pop("ASC")
    }

    fn rpop(&self) -> Result<Value, &'static str> {
        self.pop("DESC")
    }

    fn lpush(&self) -> Result<Value, &'static str> {
        let key = self.arguments[0];
        let value = self.arguments[1];

        let connection = self.lock_connection();
        connection.execute("INSERT INTO list_items (key, value, position) SELECT ?1, ?2, coalesce(MIN(position), 0) - 1 FROM list_items WHERE key = ?1", &[key, value]).unwrap();

        self.count_list_items(&*connection, key)
    }

    fn rpush(&self) -> Result<Value, &'static str> {
        let key = self.arguments[0];

        let mut connection = self.lock_connection();

        {
            let tx = connection.transaction().unwrap();

            self.arguments.iter().skip(1).map(|value|
                tx.execute("INSERT INTO list_items (key, value, position) SELECT ?1, ?2, coalesce(MAX(position), 0) + 1 FROM list_items WHERE key = ?1", &[key, *value])
            ).collect::<Result<Vec<_>, _>>().unwrap();

            tx.commit().unwrap();
        }

        self.count_list_items(&*connection, key)
    }

    // private

    fn pop(&self, order: &'static str) -> Result<Value, &'static str> {
        let key = self.arguments[0];

        let connection = self.lock_connection();
        let mut statement = connection.prepare(&format!("SELECT id, value FROM list_items WHERE key = ?1 ORDER BY position {} LIMIT 1", order)).unwrap();

        match statement.query_row(&[key], |row| (row.get(0), row.get(1))) {
            Ok(result) => {
                let (id, value): (i64, String) = result;

                connection.execute("DELETE FROM list_items WHERE id = ?1", &[&id]).unwrap();
                Ok(Value::Bulk(value))
            }

            Err(Error::QueryReturnedNoRows) => Ok(Value::NullArray),

            Err(e) => Err(e).unwrap()
        }
    }

    fn lock_connection(&self) -> MutexGuard<Connection> {
        (*self.connection_mutex).lock().unwrap()
    }

    fn count_list_items(&self, connection: &Connection, key: &String) -> Result<Value, &'static str> {
        let mut statement = connection.prepare("SELECT COUNT(*) AS c FROM list_items WHERE key = ?1").unwrap();
        let count = statement.query_row(&[key], |row| row.get(0)).unwrap();

        Ok(Value::Integer(count))
    }
}
