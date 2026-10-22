use crate::control::{ConfigAddress, ConfigValue, Mode, Stop};
use forest_protocol::Token;

pub enum Command {
    Help,
    Status,
    Packages,
    Modules,
    Show(String),
    Load(String),
    Reload(Stop),
    Unload(Stop),
    Force(String),
    Keys(String),
    Get(ConfigAddress),
    Set(ConfigValue),
}
pub const HELP: &str = include_str!("help.txt");

fn word(text: &str) -> (&str, &str) {
    let text = text.trim_start();
    let end = text.find(char::is_whitespace).unwrap_or(text.len());
    (&text[..end], text[end..].trim_start())
}
fn token(text: &str) -> Result<String, String> {
    text.parse::<Token>()
        .map(|path| path.to_string())
        .map_err(|e| format!("无效路径：{e}"))
}
fn stop(object: &str, text: &str) -> Result<Stop, String> {
    let mode = match text.trim() {
        "" | "drain" => Mode::Drain,
        "yield" => Mode::AtYield,
        "force" => Mode::Force,
        _ => return Err("重载模式应为 drain、yield 或 force".into()),
    };
    Ok(Stop {
        object: token(object)?,
        mode,
    })
}
pub fn parse(text: &str) -> Result<Command, String> {
    let (verb, rest) = word(text);
    let command = match (verb, rest) {
        ("" | "help", "") => Command::Help,
        ("status", "") => Command::Status,
        ("packages", "") => Command::Packages,
        ("module", rest) => {
            let (verb, rest) = word(rest);
            let (object, tail) = word(rest);
            match (verb, tail) {
                ("list", "") if object.is_empty() => Command::Modules,
                ("show", "") => Command::Show(token(object)?),
                ("load", "") => Command::Load(token(object)?),
                ("reload", _) => Command::Reload(stop(object, tail)?),
                ("unload", _) => Command::Unload(stop(object, tail)?),
                ("force", "") => Command::Force(token(object)?),
                _ => return Err("模块命令格式不正确，使用 #forest help 查看。".into()),
            }
        }
        ("config", rest) => {
            let (verb, rest) = word(rest);
            let (object, rest) = word(rest);
            let object = token(object)?;
            if verb == "keys" && rest.is_empty() {
                return Ok(Command::Keys(object));
            }
            let (key, value) = word(rest);
            let target = ConfigAddress {
                object,
                key: token(key)?,
            };
            match (verb, value.is_empty()) {
                ("get", true) => Command::Get(target),
                ("set", false) => Command::Set(ConfigValue {
                    target,
                    value: value.to_owned(),
                }),
                _ => return Err("配置命令格式不正确，使用 #forest help 查看。".into()),
            }
        }
        _ => return Err("未知命令，使用 #forest help 查看。".into()),
    };
    Ok(command)
}
