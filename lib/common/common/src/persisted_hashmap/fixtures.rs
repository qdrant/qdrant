use std::fmt::{Debug, Write};
use std::hash::Hash;
use std::panic::{AssertUnwindSafe, catch_unwind};

use rand::RngExt;
use rand::distr::StandardUniform;
use rand::rngs::StdRng;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::persisted_hashmap::Key;

pub trait TestKey: Key + Ord + PartialEq<Self::Owned> {
    type Owned: Clone + Ord + Hash + Debug;
    fn gen_key(rng: &mut StdRng, large: bool) -> Self::Owned;
    fn as_ref(owned: &Self::Owned) -> &Self;
    fn from_ref(this: &Self) -> Self::Owned;
}

impl TestKey for str {
    type Owned = String;
    fn gen_key(rng: &mut StdRng, large: bool) -> Self::Owned {
        let range = if large { 5_000..=10_000 } else { 5..=32 };
        (0..rng.random_range(range))
            .map(|_| rng.random_range(b'a'..=b'z') as char)
            .collect()
    }
    fn as_ref(owned: &Self::Owned) -> &Self {
        owned.as_str()
    }
    fn from_ref(this: &Self) -> Self::Owned {
        this.to_owned()
    }
}

impl TestKey for i64 {
    type Owned = i64;
    fn gen_key(rng: &mut StdRng, large: bool) -> Self::Owned {
        assert!(!large);
        rng.random()
    }
    fn as_ref(owned: &Self::Owned) -> &Self {
        owned
    }
    fn from_ref(this: &Self) -> Self::Owned {
        *this
    }
}

impl TestKey for u128 {
    type Owned = u128;
    fn gen_key(rng: &mut StdRng, large: bool) -> Self::Owned {
        assert!(!large);
        rng.random()
    }
    fn as_ref(owned: &Self::Owned) -> &Self {
        owned
    }
    fn from_ref(this: &Self) -> Self::Owned {
        *this
    }
}

pub trait TestValue:
    Copy + Ord + Debug + FromBytes + IntoBytes + Immutable + KnownLayout + Sized
{
    fn gen_value(rng: &mut StdRng) -> Self;
}
impl<T> TestValue for T
where
    T: Copy + Ord + Debug + FromBytes + IntoBytes + Immutable + KnownLayout,
    StandardUniform: rand::distr::Distribution<T>,
{
    fn gen_value(rng: &mut StdRng) -> T {
        rng.random()
    }
}

pub struct TestReport {
    success: bool,
    log: String,
}

pub struct Group<'a> {
    path: String,
    report: &'a mut TestReport,
}

impl TestReport {
    #[expect(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            success: true,
            log: String::new(),
        }
    }

    pub fn group(&mut self, name: &str) -> Group<'_> {
        Group {
            path: name.to_string(),
            report: self,
        }
    }
}

impl Drop for TestReport {
    fn drop(&mut self) {
        eprintln!("{}", self.log);
        assert!(self.success, "Some checks failed");
    }
}

impl Group<'_> {
    pub fn group(&mut self, name: &str) -> Group<'_> {
        Group {
            path: format!("{}:{name}", self.path),
            report: self.report,
        }
    }

    pub fn check(&mut self, name: &str, f: impl FnOnce()) {
        let ok = catch_unwind(AssertUnwindSafe(f)).is_ok();
        let tag = if ok { "[ OK ]" } else { "[FAIL]" };
        self.report.success &= ok;
        writeln!(self.report.log, "{tag} {}:{name}", self.path).unwrap();
    }
}
