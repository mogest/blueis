extern crate resp;
extern crate rusqlite;

use self::resp::{Value};
use self::rusqlite::{Connection};
use std::sync::{Arc, Mutex, MutexGuard};
use std::cmp;

type CommandResult = Result<Value, String>;

struct Command<'a> {
    name: String,
    arguments: Vec<&'a String>,
    connection_mutex: &'a Arc<Mutex<Connection>>
}

struct CommandSettings {
    name: &'static str,
    argument_count: i32,
    //handler: fn(&'a Command<'a>) -> CommandResult
}

const COMMAND_SETTINGS: [CommandSettings; 6] = [
    CommandSettings { name: "LLEN", argument_count: 1 }, //, handler: Command::llen }
    CommandSettings { name: "LPOP", argument_count: 1 },
    CommandSettings { name: "RPOP", argument_count: 1 },
    CommandSettings { name: "LPUSH", argument_count: -2 },
    CommandSettings { name: "RPUSH", argument_count: -2 },
    CommandSettings { name: "LRANGE", argument_count: 3 },
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
                        "LRANGE" => self.lrange(),
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

    fn llen(&self) -> CommandResult {
        let key = self.arguments[0];

        let connection = self.lock_connection();
        self.count_list_items_value(&*connection, key)
    }

    fn lpop(&self) -> CommandResult {
        self.pop("ASC")
    }

    fn rpop(&self) -> CommandResult {
        self.pop("DESC")
    }

    fn lpush(&self) -> CommandResult {
        let key = self.arguments[0];
        let value = self.arguments[1];

        let connection = self.lock_connection();
        connection.execute("INSERT INTO list_items (key, value, position) SELECT ?1, ?2, coalesce(MIN(position), 0) - 1 FROM list_items WHERE key = ?1", &[key, value]).unwrap();

        self.count_list_items_value(&*connection, key)
    }

    fn rpush(&self) -> CommandResult {
        let key = self.arguments[0];

        let mut connection = self.lock_connection();

        {
            let tx = connection.transaction().unwrap();

            self.arguments.iter().skip(1).map(|value|
                tx.execute("INSERT INTO list_items (key, value, position) SELECT ?1, ?2, coalesce(MAX(position), 0) + 1 FROM list_items WHERE key = ?1", &[key, *value])
            ).collect::<Result<Vec<_>, _>>().unwrap();

            tx.commit().unwrap();
        }

        self.count_list_items_value(&*connection, key)
    }

    fn lrange(&self) -> CommandResult {
        let key = self.arguments[0];
        let mut start: i64 = self.arguments[1].parse().map_err(|_| "start must be an integer")?;
        let mut stop: i64 = self.arguments[2].parse().map_err(|_| "stop must be an integer")?;

        let connection = self.lock_connection();

        if start < 0 || stop < -1 {
            let count = self.count_list_items(&connection, key);

            if start < 0  { start = cmp::max(0, count + start) }

            if stop < -1 {
                stop = count + stop;
                if stop < 0 { return Ok(Value::Array(vec![])); }
            }
        }

        if stop != -1 && start > stop { return Ok(Value::Array(vec![])); }

        let sql = match (start, stop) {
            (0, -1) => "".to_string(),
            (a, -1) => format!("LIMIT -1 OFFSET {}", a),
            (0, b)  => format!("LIMIT {}", b + 1),
            (a, b)  => format!("LIMIT {} OFFSET {}", b - a + 1, a),
        };

        let mut statement = connection.prepare(&format!("SELECT value FROM list_items WHERE key = ?1 ORDER BY position {}", sql)).unwrap();
        let rows = statement.query_map(&[key], |row| row.get(0)).unwrap();
        let result: Result<Vec<String>, _> = rows.collect();
        let values = result.unwrap().iter().map(|value| Value::String(value.clone())).collect();

        Ok(Value::Array(values))
    }

    // private

    fn pop(&self, order: &'static str) -> CommandResult {
        let key = self.arguments[0];

        let connection = self.lock_connection();
        let mut statement = connection.prepare(&format!("SELECT id, value FROM list_items WHERE key = ?1 ORDER BY position {} LIMIT 1", order)).unwrap();

        match statement.query_row(&[key], |row| (row.get(0), row.get(1))) {
            Ok(result) => {
                let (id, value): (i64, String) = result;

                connection.execute("DELETE FROM list_items WHERE id = ?1", &[&id]).unwrap();
                Ok(Value::Bulk(value))
            }

            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(Value::NullArray),

            Err(e) => Err(e).unwrap()
        }
    }

    fn lock_connection(&self) -> MutexGuard<Connection> {
        (*self.connection_mutex).lock().unwrap()
    }

    fn count_list_items(&self, connection: &Connection, key: &String) -> i64 {
        let mut statement = connection.prepare("SELECT COUNT(*) AS c FROM list_items WHERE key = ?1").unwrap();
        statement.query_row(&[key], |row| row.get(0)).unwrap()
    }

    fn count_list_items_value(&self, connection: &Connection, key: &String) -> CommandResult {
        Ok(Value::Integer(self.count_list_items(connection, key)))
    }
}
