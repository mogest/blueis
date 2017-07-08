extern crate resp;
extern crate rusqlite;
extern crate time;

use connection::Connection;
use self::resp::Value;
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::mpsc::Sender;
use std::time::{Instant, Duration};
use std::str;

type CommandResult = Result<Value, String>;

pub struct Command<'a> {
    pub name: &'a str,
    pub arguments: Vec<&'a [u8]>,
    pub sqlite_connection_mutex: &'a Arc<Mutex<rusqlite::Connection>>,
    pub command_log_tx: &'a Sender<String>,
    pub connection: &'a Connection,
}

struct CommandSettings {
    name: &'static str,
    argument_count: i32,
    //handler: fn(&'a Command<'a>) -> CommandResult
}

#[derive(PartialEq, Debug)]
pub enum Action {
    Continue,
    HangUp,
    StartMonitor,
}

enum Direction {
    Left,
    Right
}

const COMMAND_SETTINGS: [CommandSettings; 14] = [
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
    CommandSettings { name: "BLPOP", argument_count: -2 },
    CommandSettings { name: "BRPOP", argument_count: -2 },
];

impl<'a> Command<'a> {
    pub fn execute(&mut self) -> (Value, Action) {
        match self.name.to_string().to_uppercase().as_str() {
            "QUIT"    => (Value::String("OK".to_string()), Action::HangUp),
            "MONITOR" => (Value::String("OK".to_string()), Action::StartMonitor),
            _         => (self.handle_nonterminal_command(), Action::Continue)
        }
    }

    fn valid_argument_count(&self, command: &CommandSettings) -> bool {
        (command.argument_count < 0 && self.arguments.len() as i32 >= -command.argument_count) ||
            (command.argument_count >= 0 && self.arguments.len() as i32 == command.argument_count)
    }

    fn handle_nonterminal_command(&mut self) -> Value {
        let all = COMMAND_SETTINGS;
        let upper = self.name.to_string().to_uppercase();
        let name = upper.as_str();
        let settings = all.iter().find(|settings| settings.name == name);

        match settings {
            None => Value::Error("ERR unsupported".to_string()),

            Some(settings) => {
                if !self.valid_argument_count(settings) {
                    Value::Error("ERR wrong number of arguments".to_string())
                }
                else {
                    self.write_to_log();

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
                        "BLPOP"     => self.blpop(),
                        "BRPOP"     => self.brpop(),
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

    fn write_to_log(&self) {
        let now = time::now_utc().to_timespec();
        let args = self.arguments.iter().map(|argument| Command::quote_string(argument)).collect::<Vec<String>>().join(" ");
        let log = format!("{}.{:09} {} {}", now.sec, now.nsec, Command::quote_string(self.name.as_bytes()), args);

        self.command_log_tx.send(log).ok();
    }

    fn quote_string(input: &[u8]) -> String {
        let mut output = String::from("\"");

        for c in input {
            match *c {
                b'\\'       => output.push_str("\\\\"),
                b'"'        => output.push_str("\\\""),
                b' '...b'~' => output.push(*c as char),
                _           => output.push_str(format!("\\x{:02x}", c).as_ref())
            }
        }

        output.push('"');
        output
    }

    fn llen(&self) -> CommandResult {
        let key = self.arguments[0];

        let connection = self.lock_connection();
        self.count_list_items_value(&*connection, key)
    }

    fn lpop(&self) -> CommandResult {
        let connection = self.lock_connection();

        match Command::pop(&*connection, self.arguments[0], &Direction::Left) {
            Some(data) => Ok(Value::BufBulk(data)),
            None       => Ok(Value::Null)
        }
    }

    fn rpop(&self) -> CommandResult {
        let connection = self.lock_connection();

        match Command::pop(&*connection, self.arguments[0], &Direction::Right) {
            Some(data) => Ok(Value::BufBulk(data)),
            None       => Ok(Value::Null)
        }
    }

    fn lpush(&self) -> CommandResult {
        let key = self.arguments[0];
        let mut connection = self.lock_connection();

        Command::push(&mut *connection, key, Direction::Left, self.arguments.iter().skip(1));
        self.notify_push();

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
            self.notify_push();

            self.count_list_items_value(&*connection, key)
        }
    }

    fn rpush(&self) -> CommandResult {
        let key = self.arguments[0];
        let mut connection = self.lock_connection();

        Command::push(&mut *connection, key, Direction::Right, self.arguments.iter().skip(1));
        self.notify_push();

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
            self.notify_push();

            self.count_list_items_value(&*connection, key)
        }
    }

    fn parse_argument_integer(&self, index: usize) -> Result<i64, &str> {
        str::from_utf8(self.arguments[index])
            .map_err(|_| "")
            .and_then(|value| String::from(value).parse::<i64>().map_err(|_| ""))
            .map_err(|_| "argument must be an integer")
    }

    fn lrange(&self) -> CommandResult {
        let key = self.arguments[0];
        let start: i64 = self.parse_argument_integer(1)?;
        let stop: i64 = self.parse_argument_integer(2)?;

        let connection = self.lock_connection();

        let result: Result<Vec<Vec<u8>>, _> = match (start, stop) {
            (0, -1) => {
                let mut statement = connection.prepare("SELECT value FROM list_items WHERE key = ?1 ORDER BY position").unwrap();
                let rows = statement.query_map(&[&key], |row| row.get(0)).unwrap();
                rows.collect()
            }

            (0, s) if s >= 0 => {
                let mut statement = connection.prepare("SELECT value FROM list_items WHERE key = ?1 ORDER BY position LIMIT ?2").unwrap();
                let rows = statement.query_map(&[&key, &(stop + 1)], |row| row.get(0)).unwrap();
                rows.collect()
            }

            _ => {
                let boundaries = Command::find_position_boundaries(&*connection, key);
                let (start_position, stop_position) = Command::parse_indexes(boundaries, (start, stop));

                if start_position > stop_position {
                    return Ok(Value::Array(vec![]));
                }

                let mut statement = connection.prepare("SELECT value FROM list_items WHERE key = ?1 AND position BETWEEN ?2 AND ?3 ORDER BY position").unwrap();
                let rows = statement.query_map(&[&key, &start_position, &stop_position], |row| row.get(0)).unwrap();
                rows.collect()
            }
        };

        let values = result.unwrap().iter().map(|value| Value::BufBulk(value.clone())).collect();

        Ok(Value::Array(values))
    }

    fn ltrim(&self) -> CommandResult {
        let key = self.arguments[0];
        let start: i64 = self.parse_argument_integer(1)?;
        let stop: i64 = self.parse_argument_integer(2)?;

        if start != 0 || stop != -1 {
            let connection = self.lock_connection();

            let boundaries = Command::find_position_boundaries(&*connection, key);
            let (start_position, stop_position) = Command::parse_indexes(boundaries, (start, stop));

            connection.execute("DELETE FROM list_items WHERE key = ?1 AND (position < ?2 OR position > ?3)", &[&key, &start_position, &stop_position]).unwrap();
        }

        Ok(Value::String("OK".to_string()))
    }

    fn rpoplpush(&self) -> CommandResult {
        let source = self.arguments[0];
        let destination = self.arguments[1];

        let mut connection = self.lock_connection();

        match Command::pop(&*connection, source, &Direction::Right) {
            Some(data) => {
                Command::push(&mut *connection, destination, Direction::Left, [data.as_slice()].iter());
                self.notify_push();
                Ok(Value::BufBulk(data))
            }

            None => Ok(Value::Null)
        }
    }

    fn lindex(&self) -> CommandResult {
        let key = self.arguments[0];
        let index: i64 = self.parse_argument_integer(1)?;

        let connection = self.lock_connection();

        let boundaries = Command::find_position_boundaries(&*connection, key);
        let position = Command::parse_index(boundaries, index);

        let mut statement = connection.prepare("SELECT value FROM list_items WHERE key = ?1 AND position = ?2 LIMIT 1").unwrap();

        match statement.query_row(&[&key, &position], |row| row.get(0)) {
            Ok(data)                                  => Ok(Value::BufBulk(data)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(Value::Null),
            Err(e)                                    => panic!(e)
        }
    }

    fn lset(&self) -> CommandResult {
        let key = self.arguments[0];
        let index: i64 = self.parse_argument_integer(1)?;
        let data = self.arguments[2];

        let connection = self.lock_connection();

        let (first_position, last_position) = Command::find_position_boundaries(&*connection, key);
        let position = Command::parse_index((first_position, last_position), index);

        if position < first_position || position > last_position {
            Err("index out of range".to_string())
        }
        else {
            connection.execute("UPDATE list_items SET value = ?1 WHERE key = ?2 AND position = ?3", &[&data, &key, &position]).unwrap();
            Ok(Value::String("OK".to_string()))
        }
    }

    fn blpop(&self) -> CommandResult {
        self.blocking_pop(Direction::Left)
    }

    fn brpop(&self) -> CommandResult {
        self.blocking_pop(Direction::Right)
    }

    /*
     * support methods
     */

    fn blocking_pop(&self, direction: Direction) -> CommandResult {
        let timeout = self.parse_argument_integer(self.arguments.len() - 1)?;
        let (_, keys) = self.arguments.split_last().unwrap();

        if timeout < 0 {
            return Err("timeout is negative".to_owned());
        }

        let start_instant = Instant::now();
        let duration = Duration::new(timeout as u64, 0);

        while timeout == 0 || start_instant.elapsed() < duration {
            {
                let connection = self.lock_connection();

                for key in keys {
                    if let Some(data) = Command::pop(&*connection, key, &direction) {
                        return Ok(Value::Array(vec![Value::BufBulk(key.to_vec()), Value::BufBulk(data)]));
                    }
                }
            }

            let &(ref lock, ref cvar) = &*self.connection.push_notification;
            let guard = lock.lock().unwrap();

            if timeout == 0 {
                let _ = cvar.wait(guard).unwrap();
            }
            else {
                let elapsed = start_instant.elapsed();
                if elapsed < duration { cvar.wait_timeout(guard, duration - elapsed).unwrap(); }
            }
        }

        Ok(Value::NullArray)
    }

    fn lock_connection(&self) -> MutexGuard<rusqlite::Connection> {
        (*self.sqlite_connection_mutex).lock().unwrap()
    }

    fn count_list_items_value(&self, connection: &rusqlite::Connection, key: &[u8]) -> CommandResult {
        Ok(Value::Integer(Command::count_list_items(connection, key)))
    }

    fn notify_push(&self) {
        let &(ref lock, ref cvar) = &*self.connection.push_notification;
        let _guard = lock.lock().unwrap();
        cvar.notify_all();
    }

    /*
     * support functions
     */

    fn pop(connection: &rusqlite::Connection, key: &[u8], direction: &Direction) -> Option<Vec<u8>> {
        let order = match direction { &Direction::Left => "ASC", &Direction::Right => "DESC" };
        let mut statement = connection.prepare(&format!("SELECT id, value FROM list_items WHERE key = ?1 ORDER BY position {} LIMIT 1", order)).unwrap();

        match statement.query_row(&[&key], |row| (row.get(0), row.get(1))) {
            Ok(result) => {
                let (id, value): (i64, Vec<u8>) = result;

                connection.execute("DELETE FROM list_items WHERE id = ?1", &[&id]).unwrap();
                Some(value)
            }

            Err(rusqlite::Error::QueryReturnedNoRows) => None,

            Err(e) => Err(e).unwrap()
        }
    }

    fn push<'b, I>(connection: &mut rusqlite::Connection, key: &[u8], direction: Direction, iterator: I) -> ()
        where I: Iterator<Item=&'b &'b [u8]>
    {
        let tx = connection.transaction().unwrap();

        let next_position_sql = match direction {
            Direction::Left  => "coalesce(MIN(position), 0) - 1",
            Direction::Right => "coalesce(MAX(position), 0) + 1"
        };

        let sql = format!("INSERT INTO list_items (key, value, position) SELECT ?1, ?2, {} FROM list_items WHERE key = ?1", next_position_sql);

        iterator.map(|value| tx.execute(&sql, &[&key, value])).collect::<Result<Vec<_>, _>>().unwrap();

        tx.commit().unwrap();
    }

    fn find_position_boundaries(connection: &rusqlite::Connection, key: &[u8]) -> (i64, i64) {
        let mut statement = connection.prepare("SELECT MIN(position), MAX(position) AS c FROM list_items WHERE key = ?1").unwrap();
        statement.query_row(&[&key], |row| (row.get(0), row.get(1))).unwrap()
    }

    fn parse_indexes(boundaries: (i64, i64), (start, stop): (i64, i64)) -> (i64, i64) {
        (Command::parse_index(boundaries, start), Command::parse_index(boundaries, stop))
    }

    fn parse_index((first_position, last_position): (i64, i64), index: i64) -> i64 {
        if index < 0 { index + last_position + 1 } else { index + first_position }
    }

    fn count_list_items(connection: &rusqlite::Connection, key: &[u8]) -> i64 {
        let mut statement = connection.prepare("SELECT COUNT(*) AS c FROM list_items WHERE key = ?1").unwrap();
        statement.query_row(&[&key], |row| row.get(0)).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::Command;
    use super::Action;
    use super::rusqlite;
    use super::resp::Value;
    use connection::Connection;
    use std::sync::{Arc, Mutex};
    use std::sync::mpsc::{self, Sender};
    use std::str;

    fn make_connection() -> Arc<Mutex<rusqlite::Connection>> {
        let connection = rusqlite::Connection::open("test.sqlite3").unwrap();
        connection.execute("DROP TABLE list_items", &[]).ok();
        connection.execute("CREATE TABLE list_items (id integer primary key autoincrement, key string, value blob, position integer)", &[]).unwrap();
        connection.execute("CREATE INDEX list_items_key ON list_items(key, position)", &[]).unwrap();
        connection.execute("INSERT INTO list_items (key, value, position) VALUES (X'74657374', X'616263', -4), (X'74657374', X'646566', -5)", &[]).unwrap();

        Arc::new(Mutex::new(connection))
    }

    fn add_more_items(sqlite_connection_mutex: &Arc<Mutex<rusqlite::Connection>>) {
        let connection = sqlite_connection_mutex.lock().unwrap();
        connection.execute("INSERT INTO list_items (key, value, position) VALUES (X'74657374', X'676869', -6), (X'74657374', X'6A6B6C', -7), (X'74657374', X'6D6E6F', -8), (X'74657374', X'707172', -9), (X'74657375', X'616263', 1)", &[]).unwrap();
    }

    fn make_command<'a>(name: &'static str, arguments: &[&'a str], sqlite_connection_mutex: &'a Arc<Mutex<rusqlite::Connection>>, tx: &'a Sender<String>) -> Command<'a> {
        Command {
            name:                    name,
            arguments:               arguments.to_vec().iter().map(|arg| arg.as_bytes()).collect(),
            sqlite_connection_mutex: sqlite_connection_mutex,
            command_log_tx:          tx
        }
    }

    fn make_tx() -> Sender<String> {
        let (tx, _rx) = mpsc::channel();
        tx
    }

    fn run_command<'a>(name: &'static str, arguments: &[&'a str], sqlite_connection_mutex: &'a Arc<Mutex<rusqlite::Connection>>, tx: &'a Sender<String>, expect_action: Action) -> Value {
        let mut command = make_command(name, arguments, sqlite_connection_mutex, tx);
        let (value, action) = command.execute();
        assert_eq!(action, expect_action);
        value
    }

    fn list_key(key: &'static str, sqlite_connection_mutex: &Arc<Mutex<rusqlite::Connection>>) -> Vec<String> {
        let connection = sqlite_connection_mutex.lock().unwrap();
        let mut statement = connection.prepare("SELECT value FROM list_items WHERE key = ?1 ORDER BY position").unwrap();
        let rows = statement.query_map(&[&key.as_bytes()], |row| String::from_utf8(row.get(0)).unwrap()).unwrap();
        let result: Result<Vec<String>, _> = rows.collect();
        result.unwrap()
    }

    #[test]
    fn quit() {
        let cm = make_connection();
        let tx = make_tx();
        assert_eq!(run_command("QUIT", &[], &cm, &tx, Action::HangUp), Value::String("OK".to_string()));
    }

    #[test]
    fn llen() {
        let cm = make_connection();
        let tx = make_tx();
        assert_eq!(run_command("LLEN", &["test"], &cm, &tx, Action::Continue), Value::Integer(2));
        assert_eq!(run_command("LLEN", &["other"], &cm, &tx, Action::Continue), Value::Integer(0));
    }

    #[test]
    fn lpop() {
        let cm = make_connection();
        let tx = make_tx();
        assert_eq!(run_command("LPOP", &["test"], &cm, &tx, Action::Continue), Value::BufBulk("def".to_string().into_bytes()));
        assert_eq!(run_command("LPOP", &["test"], &cm, &tx, Action::Continue), Value::BufBulk("abc".to_string().into_bytes()));
        assert_eq!(run_command("LPOP", &["test"], &cm, &tx, Action::Continue), Value::Null);

        assert_eq!(run_command("LPOP", &["other"], &cm, &tx, Action::Continue), Value::Null);
    }

    #[test]
    fn rpop() {
        let cm = make_connection();
        let tx = make_tx();
        assert_eq!(run_command("RPOP", &["test"], &cm, &tx, Action::Continue), Value::BufBulk("abc".to_string().into_bytes()));
        assert_eq!(run_command("RPOP", &["test"], &cm, &tx, Action::Continue), Value::BufBulk("def".to_string().into_bytes()));
        assert_eq!(run_command("RPOP", &["test"], &cm, &tx, Action::Continue), Value::Null);

        assert_eq!(run_command("RPOP", &["other"], &cm, &tx, Action::Continue), Value::Null);
    }

    #[test]
    fn lpush() {
        let cm = make_connection();
        let tx = make_tx();
        assert_eq!(run_command("LPUSH", &["test", "ghi"], &cm, &tx, Action::Continue), Value::Integer(3));
        assert_eq!(run_command("LPUSH", &["test", "jkl"], &cm, &tx, Action::Continue), Value::Integer(4));
        assert_eq!(list_key("test", &cm), vec!["jkl", "ghi", "def", "abc"]);

        assert_eq!(run_command("LPUSH", &["other", "pqr"], &cm, &tx, Action::Continue), Value::Integer(1));
        assert_eq!(list_key("other", &cm), vec!["pqr"]);
    }

    #[test]
    fn lpushx() {
        let cm = make_connection();
        let tx = make_tx();
        assert_eq!(run_command("LPUSHX", &["test", "ghi"], &cm, &tx, Action::Continue), Value::Integer(3));
        assert_eq!(run_command("LPUSHX", &["test", "jkl"], &cm, &tx, Action::Continue), Value::Integer(4));
        assert_eq!(list_key("test", &cm), vec!["jkl", "ghi", "def", "abc"]);

        assert_eq!(run_command("LPUSHX", &["other", "pqr"], &cm, &tx, Action::Continue), Value::Integer(0));
        assert_eq!(list_key("other", &cm), vec![] as Vec<String>);
    }

    #[test]
    fn rpush() {
        let cm = make_connection();
        let tx = make_tx();
        assert_eq!(run_command("RPUSH", &["test", "ghi"], &cm, &tx, Action::Continue), Value::Integer(3));
        assert_eq!(run_command("RPUSH", &["test", "jkl"], &cm, &tx, Action::Continue), Value::Integer(4));
        assert_eq!(list_key("test", &cm), vec!["def", "abc", "ghi", "jkl"]);

        assert_eq!(run_command("RPUSH", &["other", "pqr"], &cm, &tx, Action::Continue), Value::Integer(1));
        assert_eq!(list_key("other", &cm), vec!["pqr"]);
    }

    #[test]
    fn rpushx() {
        let cm = make_connection();
        let tx = make_tx();
        assert_eq!(run_command("RPUSHX", &["test", "ghi"], &cm, &tx, Action::Continue), Value::Integer(3));
        assert_eq!(run_command("RPUSHX", &["test", "jkl"], &cm, &tx, Action::Continue), Value::Integer(4));
        assert_eq!(list_key("test", &cm), vec!["def", "abc", "ghi", "jkl"]);

        assert_eq!(run_command("RPUSHX", &["other", "pqr"], &cm, &tx, Action::Continue), Value::Integer(0));
        assert_eq!(list_key("other", &cm), vec![] as Vec<String>);
    }

    fn unpack(v: Value) -> Vec<String> {
        match v {
            Value::Array(array) => array.iter().map(|bufbulk|
                match *bufbulk {
                    Value::BufBulk(ref data) => String::from_utf8(data.clone()).unwrap(),
                    _ => panic!("invalid")
                }
            ).collect(),
            _ => panic!("invalid")
        }
    }

    fn run_lrange<'a>(arguments: &[&'a str], sqlite_connection_mutex: &'a Arc<Mutex<rusqlite::Connection>>, tx: &'a Sender<String>) -> Vec<String> {
        unpack(run_command("LRANGE", arguments, sqlite_connection_mutex, tx, Action::Continue))
    }

    struct LrangeCase<'a> {
        arguments: &'a [&'a str],
        expected: Vec<&'a str>
    }

    #[test]
    fn lrange() {
        let cm = make_connection();
        let tx = make_tx();
        add_more_items(&cm);

        let cases = [
            LrangeCase { arguments: &["test", "0", "-1"], expected: vec!["pqr", "mno", "jkl", "ghi", "def", "abc"] },
            LrangeCase { arguments: &["test", "0", "2"], expected: vec!["pqr", "mno", "jkl"] },
            LrangeCase { arguments: &["test", "3", "-1"], expected: vec!["ghi", "def", "abc"] },
            LrangeCase { arguments: &["test", "9", "-1"], expected: vec![] as Vec<&str> },
            LrangeCase { arguments: &["test", "3", "2"], expected: vec![] as Vec<&str> },
            LrangeCase { arguments: &["test", "-100", "-80"], expected: vec![] as Vec<&str> },
            LrangeCase { arguments: &["other", "0", "-1"], expected: vec![] as Vec<&str> },
            LrangeCase { arguments: &["test", "3", "3"], expected: vec!["ghi"] },
            LrangeCase { arguments: &["test", "3", "4"], expected: vec!["ghi", "def"] },
            LrangeCase { arguments: &["test", "3", "5"], expected: vec!["ghi", "def", "abc"] },
            LrangeCase { arguments: &["test", "3", "6"], expected: vec!["ghi", "def", "abc"] },
            LrangeCase { arguments: &["test", "3", "-3"], expected: vec!["ghi"] },
            LrangeCase { arguments: &["test", "3", "-2"], expected: vec!["ghi", "def"] },
            LrangeCase { arguments: &["test", "-3", "3"], expected: vec!["ghi"] },
            LrangeCase { arguments: &["test", "-3", "4"], expected: vec!["ghi", "def"] },
            LrangeCase { arguments: &["test", "-3", "-3"], expected: vec!["ghi"] },
            LrangeCase { arguments: &["test", "-3", "-2"], expected: vec!["ghi", "def"] },
        ];

        for case in cases.iter() {
            assert_eq!(run_lrange(&case.arguments, &cm, &tx), case.expected);
        }
    }

    #[test]
    fn ltrim() {
        let cm = make_connection();
        let tx = make_tx();
        add_more_items(&cm);

        assert_eq!(run_command("LTRIM", &["test", "0", "-1"], &cm, &tx, Action::Continue), Value::String("OK".to_string()));
        assert_eq!(list_key("test", &cm), vec!["pqr", "mno", "jkl", "ghi", "def", "abc"]);

        assert_eq!(run_command("LTRIM", &["test", "1", "-2"], &cm, &tx, Action::Continue), Value::String("OK".to_string()));
        assert_eq!(list_key("test", &cm), vec!["mno", "jkl", "ghi", "def"]);

        assert_eq!(run_command("LTRIM", &["test", "-3", "2"], &cm, &tx, Action::Continue), Value::String("OK".to_string()));
        assert_eq!(list_key("test", &cm), vec!["jkl", "ghi"]);

        assert_eq!(run_command("LTRIM", &["test", "300", "200"], &cm, &tx, Action::Continue), Value::String("OK".to_string()));
        assert_eq!(list_key("test", &cm), vec![] as Vec<String>);
    }

    #[test]
    fn rpoplpush() {
        let cm = make_connection();
        let tx = make_tx();

        assert_eq!(run_command("RPOPLPUSH", &["test", "other"], &cm, &tx, Action::Continue), Value::BufBulk("abc".to_string().into_bytes()));
        assert_eq!(list_key("test", &cm), vec!["def"]);
        assert_eq!(list_key("other", &cm), vec!["abc"]);

        assert_eq!(run_command("RPOPLPUSH", &["test", "other"], &cm, &tx, Action::Continue), Value::BufBulk("def".to_string().into_bytes()));
        assert_eq!(list_key("test", &cm), vec![] as Vec<String>);
        assert_eq!(list_key("other", &cm), vec!["def", "abc"]);

        assert_eq!(run_command("RPOPLPUSH", &["test", "other"], &cm, &tx, Action::Continue), Value::Null);
        assert_eq!(list_key("test", &cm), vec![] as Vec<String>);
        assert_eq!(list_key("other", &cm), vec!["def", "abc"]);
    }

    #[test]
    fn lindex() {
        let cm = make_connection();
        let tx = make_tx();

        assert_eq!(run_command("LINDEX", &["test", "0"], &cm, &tx, Action::Continue), Value::BufBulk("def".to_string().into_bytes()));
        assert_eq!(run_command("LINDEX", &["test", "1"], &cm, &tx, Action::Continue), Value::BufBulk("abc".to_string().into_bytes()));
        assert_eq!(run_command("LINDEX", &["test", "2"], &cm, &tx, Action::Continue), Value::Null);
        assert_eq!(run_command("LINDEX", &["test", "-1"], &cm, &tx, Action::Continue), Value::BufBulk("abc".to_string().into_bytes()));
        assert_eq!(run_command("LINDEX", &["test", "-2"], &cm, &tx, Action::Continue), Value::BufBulk("def".to_string().into_bytes()));
        assert_eq!(run_command("LINDEX", &["test", "-3"], &cm, &tx, Action::Continue), Value::Null);
    }

    #[test]
    fn lset() {
        let cm = make_connection();
        let tx = make_tx();

        assert_eq!(run_command("LSET", &["test", "0", "first"], &cm, &tx, Action::Continue), Value::String("OK".to_string()));
        assert_eq!(list_key("test", &cm), vec!["first", "abc"]);

        assert_eq!(run_command("LSET", &["test", "1", "second"], &cm, &tx, Action::Continue), Value::String("OK".to_string()));
        assert_eq!(list_key("test", &cm), vec!["first", "second"]);

        assert_eq!(run_command("LSET", &["test", "-1", "apple"], &cm, &tx, Action::Continue), Value::String("OK".to_string()));
        assert_eq!(list_key("test", &cm), vec!["first", "apple"]);

        assert_eq!(run_command("LSET", &["test", "-2", "banana"], &cm, &tx, Action::Continue), Value::String("OK".to_string()));
        assert_eq!(list_key("test", &cm), vec!["banana", "apple"]);

        assert_eq!(run_command("LSET", &["test", "-3", "nope"], &cm, &tx, Action::Continue), Value::Error("ERR index out of range".to_string()));
        assert_eq!(run_command("LSET", &["test", "2", "nope"], &cm, &tx, Action::Continue), Value::Error("ERR index out of range".to_string()));
    }
}
