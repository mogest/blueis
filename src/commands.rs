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

enum Direction {
    Left,
    Right
}

const COMMAND_SETTINGS: [CommandSettings; 8] = [
    CommandSettings { name: "LLEN", argument_count: 1 }, //, handler: Command::llen }
    CommandSettings { name: "LPOP", argument_count: 1 },
    CommandSettings { name: "RPOP", argument_count: 1 },
    CommandSettings { name: "LPUSH", argument_count: -2 },
    CommandSettings { name: "RPUSH", argument_count: -2 },
    CommandSettings { name: "LRANGE", argument_count: 3 },
    CommandSettings { name: "LTRIM", argument_count: 3 },
    CommandSettings { name: "RPOPLPUSH", argument_count: 2 },
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
                        "LTRIM" => self.ltrim(),
                        "RPOPLPUSH" => self.rpoplpush(),
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
        let connection = self.lock_connection();

        match self.pop(&*connection, self.arguments[0], Direction::Left) {
            Some(data) => Ok(Value::Bulk(data)),
            None       => Ok(Value::NullArray)
        }
    }

    fn rpop(&self) -> CommandResult {
        let connection = self.lock_connection();

        match self.pop(&*connection, self.arguments[0], Direction::Right) {
            Some(data) => Ok(Value::Bulk(data)),
            None       => Ok(Value::NullArray)
        }
    }

    fn lpush(&self) -> CommandResult {
        let key = self.arguments[0];
        let mut connection = self.lock_connection();

        self.push(&mut *connection, key, Direction::Left, self.arguments.iter().skip(1));

        self.count_list_items_value(&*connection, key)
    }

    fn rpush(&self) -> CommandResult {
        let key = self.arguments[0];
        let mut connection = self.lock_connection();

        self.push(&mut *connection, key, Direction::Right, self.arguments.iter().skip(1));

        self.count_list_items_value(&*connection, key)
    }

    fn lrange(&self) -> CommandResult {
        let key = self.arguments[0];
        let mut start: i64 = self.arguments[1].parse().map_err(|_| "start must be an integer")?;
        let mut stop: i64 = self.arguments[2].parse().map_err(|_| "stop must be an integer")?;

        let connection = self.lock_connection();

        // LATER : maybe use find_position_boundaries and parse_positions instead of this
        // this needs only one select rather than two in the case where start and stop >= 0
        // but then it uses order & limit which are probably slower on a huge table
        // investigate performance at a later date

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

    fn ltrim(&self) -> CommandResult {
        let key = self.arguments[0];
        let start: i64 = self.arguments[1].parse().map_err(|_| "start must be an integer")?;
        let stop: i64 = self.arguments[2].parse().map_err(|_| "stop must be an integer")?;

        if start != 0 || stop != -1 {
            let connection = self.lock_connection();

            let boundaries = self.find_position_boundaries(&*connection, key);
            let (start_position, stop_position) = self.parse_positions(boundaries, (start, stop));

            connection.execute("DELETE FROM list_items WHERE key = ?1 AND (position < ?2 OR position > ?3)", &[key, &start_position, &stop_position]).unwrap();
        }

        Ok(Value::String("OK".to_string()))
    }

    fn rpoplpush(&self) -> CommandResult {
        let source = self.arguments[0];
        let destination = self.arguments[1];

        let mut connection = self.lock_connection();

        match self.pop(&*connection, source, Direction::Right) {
            Some(data) => {
                self.push(&mut *connection, destination, Direction::Left, [&data].iter());
                Ok(Value::Bulk(data))
            }

            None => Ok(Value::NullArray)
        }
    }

    // private

    fn pop(&self, connection: &Connection, key: &String, direction: Direction) -> Option<String> {
        let order = match direction { Direction::Left => "ASC", Direction::Right => "DESC" };
        let mut statement = connection.prepare(&format!("SELECT id, value FROM list_items WHERE key = ?1 ORDER BY position {} LIMIT 1", order)).unwrap();

        match statement.query_row(&[key], |row| (row.get(0), row.get(1))) {
            Ok(result) => {
                let (id, value): (i64, String) = result;

                connection.execute("DELETE FROM list_items WHERE id = ?1", &[&id]).unwrap();
                Some(value)
            }

            Err(rusqlite::Error::QueryReturnedNoRows) => None,

            Err(e) => Err(e).unwrap()
        }
    }

    fn push<'b, I>(&self, connection: &mut Connection, key: &String, direction: Direction, iterator: I) -> ()
        where I: Iterator<Item=&'b &'b String>
    {
        let tx = connection.transaction().unwrap();

        let next_position_sql = match direction {
            Direction::Left  => "coalesce(MIN(position), 0) - 1",
            Direction::Right => "coalesce(MAX(position), 0) + 1"
        };

        let sql = format!("INSERT INTO list_items (key, value, position) SELECT ?1, ?2, {} FROM list_items WHERE key = ?1", next_position_sql);

        iterator.map(|value| tx.execute(&sql, &[key, *value])).collect::<Result<Vec<_>, _>>().unwrap();

        tx.commit().unwrap();
    }

    fn lock_connection(&self) -> MutexGuard<Connection> {
        (*self.connection_mutex).lock().unwrap()
    }

    fn find_position_boundaries(&self, connection: &Connection, key: &String) -> (i64, i64) {
        let mut statement = connection.prepare("SELECT MIN(position), MAX(position) AS c FROM list_items WHERE key = ?1").unwrap();
        statement.query_row(&[key], |row| (row.get(0), row.get(1))).unwrap()
    }

    fn parse_positions(&self, (first_position, last_position) : (i64, i64), (start, stop): (i64, i64)) -> (i64, i64) {
        let start_position = match start {
            position if position < 0 => cmp::max(0, position + last_position + 1),
            position                 => position + first_position
        };

        let stop_position = match stop {
            position if position < 0 => cmp::max(0, position + last_position + 1),
            position                 => position + first_position
        };

        (start_position, stop_position)
    }

    fn count_list_items(&self, connection: &Connection, key: &String) -> i64 {
        let mut statement = connection.prepare("SELECT COUNT(*) AS c FROM list_items WHERE key = ?1").unwrap();
        statement.query_row(&[key], |row| row.get(0)).unwrap()
    }

    fn count_list_items_value(&self, connection: &Connection, key: &String) -> CommandResult {
        Ok(Value::Integer(self.count_list_items(connection, key)))
    }
}
