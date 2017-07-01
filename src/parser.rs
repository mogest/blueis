extern crate resp;
extern crate rusqlite;

use self::resp::{Value};

type ParserResult<'a> = Result<(&'a str, Vec<&'a str>), &'a str>;

pub fn parse_command<'a>(value: &'a Value) -> ParserResult<'a> {
    if let &Value::Array(ref array) = value {
        parse_command_array(array)
    }
    else {
        Err("expected array")
    }
}

fn parse_command_array<'a>(array: &'a Vec<Value>) -> ParserResult<'a> {
    let iter = array.iter().map(|value|
        match *value {
            Value::String(ref string) | Value::Bulk(ref string) => Ok(string.as_str()),
            _ => Err("all arguments should be strings")
        }
    );

    let strings = iter.collect::<Result<Vec<&str>, &'static str>>()?;
    let (head, tail) = strings.split_at(1);

    Ok((head[0], tail.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::parse_command;
    use super::resp::Value;

    #[test]
    fn converts_a_valid_value() {
        let value = Value::Array(vec![Value::String("COMMAND".to_string()), Value::String("argument".to_string())]);
        let (name, arguments) = parse_command(&value).unwrap();

        assert_eq!(name, "COMMAND");
        assert_eq!(arguments, vec!["argument"]);
    }

    #[test]
    fn rejects_a_value_that_is_not_an_array() {
        let value = Value::String("COMMAND".to_string());
        assert!(parse_command(&value).is_err());
    }

    #[test]
    fn rejects_a_value_has_non_string_values_in_the_array() {
        let value = Value::Array(vec![Value::String("COMMAND".to_string()), Value::Integer(2)]);
        assert!(parse_command(&value).is_err());
    }
}
