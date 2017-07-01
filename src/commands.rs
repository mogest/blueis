extern crate resp;
extern crate rusqlite;

use self::resp::{Value};
use self::rusqlite::{Connection};
use std::sync::{Arc, Mutex, MutexGuard};
use std::cmp;

type CommandResult = Result<Value, String>;

pub struct Command<'a> {
    pub name: String,
    pub arguments: Vec<&'a String>,
    pub connection_mutex: &'a Arc<Mutex<Connection>>
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

const COMMAND_SETTINGS: [CommandSettings; 12] = [
    CommandSettings { name: "LLEN", argument_count: 1 }, //, handler: Command::llen }
    CommandSettings { name: "LPOP", argument_count: 1 },
    CommandSettings { name: "RPOP", argument_count: 1 },
    CommandSettings { name: "LPUSH", argument_count: -2 },
    CommandSettings { name: "LPUSHX", argument_count: -2 },
    CommandSettings { name: "RPUSH", argument_count: -2 },
    CommandSettings { name: "RPUSHX", argument_count: -2 },
    CommandSettings { name: "LRANGE", argument_count: 3 },
    CommandSettings { name: "LTRIM", argument_count: 3 },
    CommandSettings { name: "RPOPLPUSH", argument_count: 2 },
    CommandSettings { name: "LINDEX", argument_count: 2 },
    CommandSettings { name: "LSET", argument_count: 3 },
];

impl<'a> Command<'a> {
    pub fn handle_command(&mut self) -> (Value, bool) {
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
            None => Value::Error("ERR unsupported".to_string()),

            Some(settings) => {
                if !self.valid_argument_count(settings) {
                    Value::Error("ERR wrong number of arguments".to_string())
                }
                else {
                    let result = match settings.name {
                        "LLEN"      => self.llen(),
                        "LPUSH"     => self.lpush(),
                        "LPUSHX"    => self.lpushx(),
                        "RPUSH"     => self.rpush(),
                        "RPUSHX"    => self.rpushx(),
                        "LPOP"      => self.lpop(),
                        "RPOP"      => self.rpop(),
                        "LRANGE"    => self.lrange(),
                        "LTRIM"     => self.ltrim(),
                        "RPOPLPUSH" => self.rpoplpush(),
                        "LINDEX"    => self.lindex(),
                        "LSET"      => self.lset(),
                        _           => unimplemented!(),
                    };

                    match result {
                        Ok(value)  => value,
                        Err(error) => Value::Error(format!("ERR {}", error))
                    }
                }
            }
        }
    }

    fn llen(&self) -> CommandResult {
        let key = self.arguments[0];

        let connection = self.lock_connection();
        self.count_list_items_value(&*connection, key)
    }

    fn lpop(&self) -> CommandResult {
        let connection = self.lock_connection();

        match Command::pop(&*connection, self.arguments[0], Direction::Left) {
            Some(data) => Ok(Value::Bulk(data)),
            None       => Ok(Value::NullArray)
        }
    }

    fn rpop(&self) -> CommandResult {
        let connection = self.lock_connection();

        match Command::pop(&*connection, self.arguments[0], Direction::Right) {
            Some(data) => Ok(Value::Bulk(data)),
            None       => Ok(Value::NullArray)
        }
    }

    fn lpush(&self) -> CommandResult {
        let key = self.arguments[0];
        let mut connection = self.lock_connection();

        Command::push(&mut *connection, key, Direction::Left, self.arguments.iter().skip(1));

        self.count_list_items_value(&*connection, key)
    }

    fn lpushx(&self) -> CommandResult {
        let key = self.arguments[0];
        let mut connection = self.lock_connection();

        if Command::count_list_items(&*connection, key) == 0 {
            Ok(Value::Integer(0))
        }
        else {
            Command::push(&mut *connection, key, Direction::Left, self.arguments.iter().skip(1));

            self.count_list_items_value(&*connection, key)
        }
    }

    fn rpush(&self) -> CommandResult {
        let key = self.arguments[0];
        let mut connection = self.lock_connection();

        Command::push(&mut *connection, key, Direction::Right, self.arguments.iter().skip(1));

        self.count_list_items_value(&*connection, key)
    }

    fn rpushx(&self) -> CommandResult {
        let key = self.arguments[0];
        let mut connection = self.lock_connection();

        if Command::count_list_items(&*connection, key) == 0 {
            Ok(Value::Integer(0))
        }
        else {
            Command::push(&mut *connection, key, Direction::Right, self.arguments.iter().skip(1));

            self.count_list_items_value(&*connection, key)
        }
    }

    fn lrange(&self) -> CommandResult {
        let key = self.arguments[0];
        let mut start: i64 = self.arguments[1].parse().map_err(|_| "start must be an integer")?;
        let mut stop: i64 = self.arguments[2].parse().map_err(|_| "stop must be an integer")?;

        let connection = self.lock_connection();

        // LATER : maybe use find_position_boundaries and parse_indexes instead of this
        // this needs only one select rather than two in the case where start and stop >= 0
        // but then it uses order & limit which are probably slower on a huge table
        // investigate performance at a later date

        if start < 0 || stop < -1 {
            let count = Command::count_list_items(&connection, key);

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

            let boundaries = Command::find_position_boundaries(&*connection, key);
            let (start_position, stop_position) = Command::parse_indexes(boundaries, (start, stop));

            connection.execute("DELETE FROM list_items WHERE key = ?1 AND (position < ?2 OR position > ?3)", &[key, &start_position, &stop_position]).unwrap();
        }

        Ok(Value::String("OK".to_string()))
    }

    fn rpoplpush(&self) -> CommandResult {
        let source = self.arguments[0];
        let destination = self.arguments[1];

        let mut connection = self.lock_connection();

        match Command::pop(&*connection, source, Direction::Right) {
            Some(data) => {
                Command::push(&mut *connection, destination, Direction::Left, [&data].iter());
                Ok(Value::Bulk(data))
            }

            None => Ok(Value::NullArray)
        }
    }

    fn lindex(&self) -> CommandResult {
        let key = self.arguments[0];
        let index: i64 = self.arguments[1].parse().map_err(|_| "index must be an integer")?;

        let connection = self.lock_connection();

        let boundaries = Command::find_position_boundaries(&*connection, key);
        let position = Command::parse_index(boundaries, index);

        let mut statement = connection.prepare("SELECT value FROM list_items WHERE key = ?1 AND position = ?2 LIMIT 1").unwrap();

        match statement.query_row(&[key, &position], |row| row.get(0)) {
            Ok(data)                                  => Ok(Value::Bulk(data)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(Value::NullArray),
            Err(e)                                    => panic!(e)
        }
    }

    fn lset(&self) -> CommandResult {
        let key = self.arguments[0];
        let index: i64 = self.arguments[1].parse().map_err(|_| "index must be an integer")?;
        let data = self.arguments[2];

        let connection = self.lock_connection();

        let (first_position, last_position) = Command::find_position_boundaries(&*connection, key);
        let position = Command::parse_index((first_position, last_position), index);

        if position < first_position || position > last_position {
            Err("index out of range".to_string())
        }
        else {
            connection.execute("UPDATE list_items SET value = ?1 WHERE key = ?2 AND position = ?3", &[data, key, &position]).unwrap();
            Ok(Value::String("OK".to_string()))
        }
    }

    /*
     * support methods
     */

    fn lock_connection(&self) -> MutexGuard<Connection> {
        (*self.connection_mutex).lock().unwrap()
    }

    fn count_list_items_value(&self, connection: &Connection, key: &String) -> CommandResult {
        Ok(Value::Integer(Command::count_list_items(connection, key)))
    }

    /*
     * support functions
     */

    fn pop(connection: &Connection, key: &String, direction: Direction) -> Option<String> {
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

    fn push<'b, I>(connection: &mut Connection, key: &String, direction: Direction, iterator: I) -> ()
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

    fn find_position_boundaries(connection: &Connection, key: &String) -> (i64, i64) {
        let mut statement = connection.prepare("SELECT MIN(position), MAX(position) AS c FROM list_items WHERE key = ?1").unwrap();
        statement.query_row(&[key], |row| (row.get(0), row.get(1))).unwrap()
    }

    fn parse_indexes(boundaries: (i64, i64), (start, stop): (i64, i64)) -> (i64, i64) {
        (Command::parse_index(boundaries, start), Command::parse_index(boundaries, stop))
    }

    fn parse_index((first_position, last_position): (i64, i64), index: i64) -> i64 {
        if index < 0 { index + last_position + 1 } else { index + first_position }
    }

    fn count_list_items(connection: &Connection, key: &String) -> i64 {
        let mut statement = connection.prepare("SELECT COUNT(*) AS c FROM list_items WHERE key = ?1").unwrap();
        statement.query_row(&[key], |row| row.get(0)).unwrap()
    }
}
