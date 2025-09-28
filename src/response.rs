use crate::request::Command;

pub fn process_command(command: &Command) -> &[u8] {
    match command {
        Command::PING => b"+PONG\r\n",
    }
}
