use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use crate::types::Match;

pub const HEAP_CAPACITY: usize = 23;

/// Small string optimization with Arc for heap case
/// Arc is used instead of Rc for Send trait that is required for collection
/// Read more about Arc<str> here:
/// https://github.com/rust-lang/rfcs/blob/master/text/1845-shared-from-slice.md
#[derive(Eq, Clone)]
pub enum MapIndexString {
    Heap(Arc<str>),
    Stack([u8; HEAP_CAPACITY]),
}

impl AsRef<str> for MapIndexString {
    fn as_ref(&self) -> &str {
        match self {
            Self::Heap(s) => s.as_ref(),
            Self::Stack(arr) => std::str::from_utf8(&arr[1..1 + arr[0] as usize]).unwrap(),
        }
    }
}

impl Into<String> for MapIndexString {
    fn into(self) -> String {
        self.as_ref().to_owned()
    }
}

impl Into<Match> for MapIndexString {
    fn into(self) -> Match {
        let s: String = self.into();
        s.into()
    }
}

impl From<String> for MapIndexString {
    fn from(value: String) -> Self {
        let bytes_count = value.as_bytes().len();
        if bytes_count > HEAP_CAPACITY - 2 {
            MapIndexString::Heap(value.into_boxed_str().into())
        } else {
            let mut arr: [u8; HEAP_CAPACITY] = [0; HEAP_CAPACITY];
            arr[0] = bytes_count as u8;
            arr[1..bytes_count + 1].copy_from_slice(value.as_bytes());
            MapIndexString::Stack(arr)
        }
    }
}

impl From<&str> for MapIndexString {
    fn from(value: &str) -> Self {
        let bytes_count = value.as_bytes().len();
        if bytes_count > HEAP_CAPACITY - 2 {
            MapIndexString::Heap(value.into())
        } else {
            let mut arr: [u8; HEAP_CAPACITY] = [0; HEAP_CAPACITY];
            arr[0] = bytes_count as u8;
            arr[1..bytes_count + 1].copy_from_slice(value.as_bytes());
            MapIndexString::Stack(arr)
        }
    }
}

impl Hash for MapIndexString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let s: &str = self.as_ref();
        s.hash(state)
    }
}

impl PartialEq for MapIndexString {
    fn eq(&self, other: &Self) -> bool {
        let s1: &str = self.as_ref();
        let s2: &str = other.as_ref();
        s1.eq(s2)
    }
}

impl PartialEq<String> for MapIndexString {
    fn eq(&self, other: &String) -> bool {
        let s1: &str = self.as_ref();
        let s2: &str = other.as_ref();
        s1.eq(s2)
    }
}

impl Display for MapIndexString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: &str = self.as_ref();
        s.fmt(f)
    }
}

impl FromStr for MapIndexString {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(MapIndexString::from(value))
    }
}

impl std::borrow::Borrow<str> for MapIndexString {
    fn borrow(&self) -> &str {
        self.as_ref()
    }
}

impl PartialEq<MapIndexString> for String {
    fn eq(&self, other: &MapIndexString) -> bool {
        self.eq(other.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_index_string_convertions() {
        let strings = [
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσουΚόσμεこんにちは世界",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσουΚόσμεこんにちは世",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσουΚόσμεこんにちは",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσουΚόσμεこんにち",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσουΚόσμεこんに",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσουΚόσμεこん",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσουΚόσμεこ",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσουΚόσμε",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσουΚόσμ",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσουΚόσ",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσουΚό",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσουΚ",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσου",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσο",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειάσ",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓειά",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓει",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓε",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالمΓ",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعالم",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعال",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالعا",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بالع",
            "ΓειάσουΚόσμεこんにちは世界مرحبا بال",
            "ΓειάσουΚόσμεこんにちは世界مرحبا با",
            "ΓειάσουΚόσμεこんにちは世界مرحبا ب",
            "ΓειάσουΚόσμεこんにちは世界مرحبا",
            "ΓειάσουΚόσμεこんにちは世界مرحب",
            "ΓειάσουΚόσμεこんにちは世界مرح",
            "ΓειάσουΚόσμεこんにちは世界مر",
            "ΓειάσουΚόσμεこんにちは世界م",
            "ΓειάσουΚόσμεこんにちは世界",
            "ΓειάσουΚόσμεこんにちは世",
            "ΓειάσουΚόσμεこんにちは",
            "ΓειάσουΚόσμεこんにち",
            "ΓειάσουΚόσμεこんに",
            "ΓειάσουΚόσμεこん",
            "ΓειάσουΚόσμεこ",
            "ΓειάσουΚόσμε",
            "ΓειάσουΚόσμ",
            "ΓειάσουΚόσ",
            "ΓειάσουΚό",
            "ΓειάσουΚ",
            "Γειάσου",
            "Γειάσο",
            "Γειάσ",
            "Γειά",
            "Γει",
            "Γε",
            "Γ",
            "",
        ];
        for s in strings {
            for i in 0..HEAP_CAPACITY {
                let mut string = std::iter::repeat("X").take(i).collect::<String>();
                string.push_str(s);
                let map_string: MapIndexString = s.into();
                assert_eq!(s, map_string.as_ref());
            }
        }
    }
}
