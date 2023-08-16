use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, JsonSchema, Debug)]
struct StackTraceSymbol {
    name: Option<String>,
    file: Option<String>,
    line: Option<u32>,
}

#[derive(Deserialize, Serialize, JsonSchema, Debug)]
struct StackTraceFrame {
    symbols: Vec<StackTraceSymbol>,
}

impl StackTraceFrame {
    pub fn render(&self) -> String {
        let mut result = String::new();
        for symbol in &self.symbols {
            let symbol_string = format!(
                "{}:{} - {} ",
                symbol.file.as_deref().unwrap_or_default(),
                symbol.line.unwrap_or_default(),
                symbol.name.as_deref().unwrap_or_default(),
            );
            result.push_str(&symbol_string);
        }
        result
    }
}

#[derive(Deserialize, Serialize, JsonSchema, Debug)]
pub struct ThreadStackTrace {
    id: u32,
    name: String,
    frames: Vec<String>,
}

#[derive(Deserialize, Serialize, JsonSchema, Debug)]
pub struct StackTrace {
    threads: Vec<ThreadStackTrace>,
}

pub fn get_stack_trace() -> StackTrace {
    #[cfg(not(feature = "stacktrace"))]
    {
        StackTrace { threads: vec![] }
    }

    #[cfg(feature = "stacktrace")]
    {
        let exe = std::env::current_exe().unwrap();
        let trace =
            rstack_self::trace(std::process::Command::new(exe).arg("--stacktrace")).unwrap();
        StackTrace {
            threads: trace
                .threads()
                .iter()
                .map(|thread| ThreadStackTrace {
                    id: thread.id(),
                    name: thread.name().to_string(),
                    frames: thread
                        .frames()
                        .iter()
                        .map(|frame| {
                            let frame = StackTraceFrame {
                                symbols: frame
                                    .symbols()
                                    .iter()
                                    .map(|symbol| StackTraceSymbol {
                                        name: symbol.name().map(|name| name.to_string()),
                                        file: symbol.file().map(|file| {
                                            file.to_str().unwrap_or_default().to_string()
                                        }),
                                        line: symbol.line(),
                                    })
                                    .collect(),
                            };
                            frame.render()
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}
