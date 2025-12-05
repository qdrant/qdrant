use std::collections::HashMap;
use std::fmt;

#[derive(Copy, Clone)]
pub struct ReprDisplay<T>(pub T);

impl<T: Repr> fmt::Display for ReprDisplay<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Repr::fmt(&self.0, f)
    }
}

pub trait Repr {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result;

    fn repr(&self) -> String {
        let mut repr = String::new();
        self.fmt(&mut repr).expect("infallible");
        repr
    }
}

impl<T: Repr + ?Sized> Repr for &T {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        Repr::fmt(*self, f)
    }
}

impl Repr for bool {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        write!(f, "{}", if *self { "True" } else { "False" })
    }
}

impl Repr for u32 {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Repr for u64 {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Repr for usize {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Repr for f32 {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Repr for str {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Repr for String {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl Repr for uuid::Uuid {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        write!(f, "\"{self}\"")
    }
}

impl<T: Repr> Repr for Option<T> {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        match self {
            Some(value) => value.fmt(f),
            None => write!(f, "None"),
        }
    }
}

impl<T: Repr> Repr for [T] {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        write!(f, "[")?;

        for value in self {
            write!(f, "{}, ", ReprDisplay(value))?;
        }

        write!(f, "]")?;

        Ok(())
    }
}

impl<T: Repr> Repr for Vec<T> {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}

impl<K: Repr, V: Repr> Repr for HashMap<K, V> {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        write!(f, "{{ ")?;

        for (key, value) in self {
            write!(f, "{}: {}, ", ReprDisplay(key), ReprDisplay(value))?;
        }

        write!(f, "}}")?;

        Ok(())
    }
}

impl Repr for serde_json::Value {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        match self {
            serde_json::Value::Null => write!(f, "None"),
            serde_json::Value::Bool(bool) => bool.fmt(f),
            serde_json::Value::Number(num) => num.fmt(f),
            serde_json::Value::String(str) => str.fmt(f),
            serde_json::Value::Array(array) => array.fmt(f),
            serde_json::Value::Object(object) => object.fmt(f),
        }
    }
}

impl Repr for serde_json::Number {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Repr for serde_json::Map<String, serde_json::Value> {
    fn fmt(&self, f: &mut dyn fmt::Write) -> fmt::Result {
        write!(f, "{{ ")?;

        for (key, value) in self.iter() {
            write!(f, "{}: {}, ", ReprDisplay(key), ReprDisplay(value))?;
        }

        write!(f, "}}")?;

        Ok(())
    }
}
