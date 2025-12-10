use std::collections::HashMap;
use std::fmt;

use pyo3::PyTypeInfo;

pub trait Repr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result;

    fn repr(&self) -> String {
        let mut repr = String::new();
        self.fmt(&mut repr).expect("infallible");
        repr
    }
}

pub type Formatter<'a> = dyn fmt::Write + 'a;

impl<T: Repr + ?Sized> Repr for &T {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Repr::fmt(*self, f)
    }
}

impl Repr for bool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", if *self { "True" } else { "False" })
    }
}

impl Repr for u64 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Repr for usize {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Repr for f32 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Repr for str {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Repr for String {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl<K: AsRef<str>, V: Repr, S> Repr for HashMap<K, V, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.map(self)
    }
}

impl<T: Repr> Repr for Option<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Some(value) => value.fmt(f),
            None => write!(f, "None"),
        }
    }
}

impl Repr for uuid::Uuid {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "\"{self}\"")
    }
}

pub trait WriteExt: fmt::Write {
    fn class<T: PyTypeInfo>(&mut self, fields: &[(&str, &dyn Repr)]) -> fmt::Result {
        write!(self, "{}(", T::NAME)?;

        let mut separator = "";
        for (field, value) in fields {
            write!(self, "{separator}{field}={}", ReprFmt(value))?;
            separator = ", ";
        }

        write!(self, ")")?;

        Ok(())
    }

    fn complex_enum<T: PyTypeInfo>(
        &mut self,
        variant: &str,
        fields: &[(&str, &dyn Repr)],
    ) -> fmt::Result {
        write!(self, "{}.{}(", T::NAME, variant)?;

        let mut separator = "";
        for (field, value) in fields {
            write!(self, "{separator}{field}={}", ReprFmt(value))?;
            separator = ", ";
        }

        write!(self, ")")?;

        Ok(())
    }

    fn simple_enum<T: PyTypeInfo>(&mut self, variant: &str) -> fmt::Result {
        write!(self, "{}.{}", T::NAME, variant)
    }

    fn list<T: Repr>(&mut self, list: impl IntoIterator<Item = T>) -> fmt::Result {
        write!(self, "[")?;

        let mut separator = "";
        for value in list {
            write!(self, "{separator}{}", ReprFmt(value))?;
            separator = ", ";
        }

        write!(self, "]")?;

        Ok(())
    }

    fn map<K, V>(&mut self, map: impl IntoIterator<Item = (K, V)>) -> fmt::Result
    where
        K: AsRef<str>,
        V: Repr,
    {
        write!(self, "{{")?;

        let mut separator = "";
        for (key, value) in map {
            write!(
                self,
                "{separator}{}: {}",
                ReprFmt(key.as_ref()),
                ReprFmt(value)
            )?;

            separator = ", ";
        }

        write!(self, "}}")?;

        Ok(())
    }

    fn set<T: Repr>(&mut self, set: impl IntoIterator<Item = T>) -> fmt::Result {
        let mut set = set.into_iter().peekable();

        if set.peek().is_none() {
            self.write_str("set()")?;
            return Ok(());
        }

        write!(self, "{{")?;

        let mut separator = "";
        for value in set {
            write!(self, "{separator}{}", ReprFmt(value))?;
            separator = ", ";
        }

        write!(self, "}}")?;

        Ok(())
    }

    fn unimplemented(&mut self) -> fmt::Result {
        self.write_str("UNIMPLEMENTED")
    }
}

impl<W: fmt::Write> WriteExt for W {}
impl<'a> WriteExt for dyn fmt::Write + 'a {}

#[derive(Copy, Clone)]
struct ReprFmt<T>(pub T);

impl<T: Repr> fmt::Display for ReprFmt<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Repr::fmt(&self.0, f)
    }
}
