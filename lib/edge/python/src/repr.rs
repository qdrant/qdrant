use std::collections::HashMap;
use std::fmt;

#[derive(Copy, Clone)]
pub struct Repr<T>(pub T);

impl fmt::Display for Repr<bool> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", if self.0 { "True" } else { "False" })
    }
}

impl fmt::Display for Repr<&String> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl<T: fmt::Display> fmt::Display for Repr<Option<T>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(value) => value.fmt(f),
            None => write!(f, "None"),
        }
    }
}

impl<T: fmt::Display> fmt::Display for Repr<&HashMap<String, T>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{ ")?;

        for (key, value) in self.0 {
            write!(f, "{}: {value}, ", Repr(key))?;
        }

        write!(f, "}}")?;

        Ok(())
    }
}
