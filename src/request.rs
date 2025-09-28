use crate::parser::RedisValue;

pub enum Command {
    PING,
}

pub enum ParsedCommand {
    Command(Command),
    NotEnoughBytes,
}

pub fn parse_command(_value: RedisValue) -> ParsedCommand {
    ParsedCommand::Command(Command::PING)
}
