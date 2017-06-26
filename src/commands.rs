extern crate resp;
extern crate rusqlite;

use self::resp::{Value};
use self::rusqlite::Connection;

struct Command<'a> {
    name: String,
    arguments: Vec<&'a String>,
    connection: &'a mut Connection
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

pub fn handle_input(ref value: Value, connection: &mut Connection) -> (Value, bool) {
    match parse_command(value, connection) {
        Ok(mut command) => { command.handle_command() }
        Err(error)  => { (Value::Error(format!("ERR {}", error)), false) }
    }
}

fn parse_command<'a>(value: &'a Value, connection: &'a mut Connection) -> Result<Command<'a>, &'static str> {
    if let &Value::Array(ref array) = value {
        parse_command_array(array, connection)
    }
    else {
        Err("expected array")
    }
}

fn parse_command_array<'a>(array: &'a Vec<Value>, connection: &'a mut Connection) -> Result<Command<'a>, &'static str> {
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
        connection: connection
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
                        _ => panic!("hmph"),
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

    fn llen(&mut self) -> Result<Value, &'static str> {
        let key = self.arguments[0];

        let mut statement = self.connection.prepare("SELECT COUNT(*) AS c FROM list_items WHERE key = ?1").unwrap();
        let result = statement.query_row(&[key], |row| row.get(0)).unwrap();

        Ok(Value::Integer(result))
    }

    fn lpop(&mut self) -> Result<Value, &'static str> {
        self.pop("ASC")
    }

    fn rpop(&mut self) -> Result<Value, &'static str> {
        self.pop("DESC")
    }

    fn pop(&mut self, order: &'static str) -> Result<Value, &'static str> {
        let key = self.arguments[0];

        // TODO : LOCK

        let mut statement = self.connection.prepare(&format!("SELECT id, value FROM list_items WHERE key = ?1 ORDER BY position {} LIMIT 1", order)).unwrap();

        match statement.query_row(&[key], |row| (row.get(0), row.get(1))) {
            Ok(result) => {
                let (id, value): (i64, String) = result;

                self.connection.execute("DELETE FROM list_items WHERE id = ?1", &[&id]);
                Ok(Value::Bulk(value))
            }

            Err(e) => {
                Ok(Value::NullArray)
            }
        }
    }

    fn lpush(&mut self) -> Result<Value, &'static str> {
        let key = self.arguments[0];
        let value = self.arguments[1];

        self.connection.execute("INSERT INTO list_items (key, value, position) SELECT ?1, ?2, coalesce(MIN(position), 0) - 1 FROM list_items WHERE key = ?1", &[key, value]).unwrap();

        self.llen()
    }

    fn rpush(&mut self) -> Result<Value, &'static str> {
        let key = self.arguments[0];

        {
            let tx = self.connection.transaction().unwrap();

            self.arguments.iter().skip(1).map(|value|
                tx.execute("INSERT INTO list_items (key, value, position) SELECT ?1, ?2, coalesce(MAX(position), 0) + 1 FROM list_items WHERE key = ?1", &[key, *value])
            ).collect::<Result<Vec<_>, _>>();

            tx.commit();
        }

        self.llen()
    }
}
