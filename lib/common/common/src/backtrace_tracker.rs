use std::backtrace::{Backtrace, BacktraceFrame};
use std::sync::Arc;

#[derive(Clone)]
pub struct BacktraceTracker {
    fn_regex: Arc<regex::Regex>,
}

impl BacktraceTracker {
    pub fn new() -> Self {
        let fn_regex = regex::Regex::new(r#"fn: "([^"]+)"#).unwrap();
        Self {
            fn_regex: Arc::new(fn_regex),
        }
    }

    pub fn get_backtraces(&self, n: usize) -> Callstack {
        let backtraces = self.get_backtraces_filtered(n);
        if backtraces.functions.is_empty() {
            let res = Backtrace::force_capture()
                .frames()
                .iter()
                .map(|i| self.extract_functions(&i))
                .filter(|i| !i.is_empty())
                .flatten()
                .collect::<Vec<_>>();
            return Callstack { functions: res };
        }

        backtraces
    }

    fn get_backtraces_filtered(&self, n: usize) -> Callstack {
        let mut functions = Backtrace::force_capture()
            .frames()
            .iter()
            .map(|i| self.extract_functions(&i))
            .filter(|i| !i.is_empty())
            .skip(1)
            .take_while(|i| !i.iter().any(|j| j.starts_with("core::")))
            .take(n + 1)
            .flatten()
            .collect::<Vec<_>>();

        functions.reverse();
        Callstack { functions }
    }

    fn extract_functions(&self, frame: &BacktraceFrame) -> Vec<String> {
        let s = format!("{frame:?}");
        self.fn_regex
            .captures_iter(&s)
            .into_iter()
            .map(|i| i[1].to_string())
            .filter(|i| {
                !i.starts_with("std::")
                    && !i.starts_with("<core::")
                    && !i.starts_with("__rust")
                    && !i.starts_with("tokio::")
                    && !i.starts_with("<tokio::")
            })
            .map(|i| {
                i.strip_suffix("::{{closure}}")
                    .map(|i| i.to_string())
                    .unwrap_or(i)
            })
            .collect()
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Callstack {
    pub(super) functions: Vec<String>,
}
