use crate::areas::{MemoryArea, Protection, ShareMode};
use crate::error::Error;
use crate::os_impl::unix::MmapOptions;
use crate::PageSizes;
use combine::{
    error::ParseError,
    parser::{
        char::{digit, hex_digit, string},
        repeat::many1,
    },
    token, EasyParser, Parser, Stream,
};
use std::fs::File;
use std::io::Lines;
use std::io::{BufRead, BufReader};
use std::ops::Range;
use std::path::PathBuf;

fn hugepage_path<Input>() -> impl Parser<Input, Output = u32>
where
    Input: Stream<Token = char>,
    <Input::Error as ParseError<Input::Token, Input::Range, Input::Position>>::StreamError:
        From<::std::num::ParseIntError>,
{
    (
        string("hugepages-"),
        many1(digit()).and_then(|s: String| s.parse::<usize>()),
        string("kB"),
    )
        .map(|(_, size, _)| size.ilog2() + 10)
}

fn hex_digit1<Input>() -> impl Parser<Input, Output = String>
where
    Input: Stream<Token = char>,
{
    many1(hex_digit())
}

fn address_range<Input>() -> impl Parser<Input, Output = Range<usize>>
where
    Input: Stream<Token = char>,
    <Input::Error as ParseError<Input::Token, Input::Range, Input::Position>>::StreamError:
        From<::std::num::ParseIntError>,
{
    (
        hex_digit1().and_then(|s| usize::from_str_radix(s.as_str(), 16)),
        token('-'),
        hex_digit1().and_then(|s| usize::from_str_radix(s.as_str(), 16)),
    )
        .map(|(start, _, end)| start..end)
}

fn permissions<Input>() -> impl Parser<Input, Output = (Protection, ShareMode)>
where
    Input: Stream<Token = char>,
{
    use combine::parser::{char::char, choice::or};

    (
        or(
            char('r').map(|_| Protection::READ),
            char('-').map(|_| Protection::empty()),
        ),
        or(
            char('w').map(|_| Protection::WRITE),
            char('-').map(|_| Protection::empty()),
        ),
        or(
            char('x').map(|_| Protection::EXECUTE),
            char('-').map(|_| Protection::empty()),
        ),
        or(
            char('s').map(|_| ShareMode::Shared),
            char('p').map(|_| ShareMode::Private),
        ),
    )
        .map(|(r, w, x, s)| (r | w | x, s))
}

fn device_id<Input>() -> impl Parser<Input, Output = (u8, u8)>
where
    Input: Stream<Token = char>,
    <Input::Error as ParseError<Input::Token, Input::Range, Input::Position>>::StreamError:
        From<::std::num::ParseIntError>,
{
    (
        hex_digit1().and_then(|s| u8::from_str_radix(s.as_str(), 16)),
        token(':'),
        hex_digit1().and_then(|s| u8::from_str_radix(s.as_str(), 16)),
    )
        .map(|(major, _, minor)| (major, minor))
}

fn path<Input>() -> impl Parser<Input, Output = PathBuf>
where
    Input: Stream<Token = char>,
{
    use combine::parser::token::satisfy;

    many1(satisfy(|c| c != '\n')).map(|s: String| PathBuf::from(s))
}

fn memory_region<Input>() -> impl Parser<Input, Output = MemoryArea>
where
    Input: Stream<Token = char>,
    <Input::Error as ParseError<Input::Token, Input::Range, Input::Position>>::StreamError:
        From<::std::num::ParseIntError>,
{
    use combine::parser::{char::spaces, choice::optional};

    (
        address_range(),
        spaces(),
        permissions(),
        spaces(),
        hex_digit1().and_then(|s| u64::from_str_radix(s.as_str(), 16)),
        spaces(),
        device_id(),
        spaces(),
        hex_digit1(),
        spaces(),
        optional(path()),
    )
        .map(
            |(range, _, (protection, share_mode), _, offset, _, _, _, _, _, path)| MemoryArea {
                allocation_base: range.start,
                range,
                protection,
                share_mode,
                path: path.map(|path| (path, offset)),
            },
        )
}

impl MmapOptions<'_> {
    pub fn page_sizes() -> Result<PageSizes, Error> {
        let mut sizes = 1 << Self::page_size().ilog2();

        if let Ok(dir) = std::fs::read_dir("/sys/kernel/mm/hugepages") {
            for entry in dir {
                let entry = match entry {
                    Ok(entry) => entry,
                    _ => continue,
                };

                let name = match entry.file_name().into_string() {
                    Ok(name) => name,
                    _ => continue,
                };

                use combine::stream::position::Stream;

                let size = match hugepage_path().easy_parse(Stream::new(name.as_str())) {
                    Ok((size, _)) => size,
                    _ => continue,
                };

                sizes |= 1 << size;
            }
        }

        Ok(PageSizes::from_bits_truncate(sizes))
    }
}

pub struct MemoryAreas<B> {
    lines: Lines<B>,
    range: Option<Range<usize>>,
}

impl MemoryAreas<BufReader<File>> {
    pub fn open(pid: Option<u32>, range: Option<Range<usize>>) -> Result<Self, Error> {
        let path = match pid {
            Some(pid) => format!("/proc/{}/maps", pid),
            _ => "/proc/self/maps".to_string(),
        };

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let lines = reader.lines();

        Ok(Self { lines, range })
    }
}

impl<B: BufRead> Iterator for MemoryAreas<B> {
    type Item = Result<MemoryArea, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = match self.lines.next() {
                Some(Ok(line)) => line,
                Some(Err(e)) => return Some(Err(Error::Io(e))),
                None => return None,
            };

            use combine::stream::position::Stream;

            let region = match memory_region().easy_parse(Stream::new(line.as_str())) {
                Ok((region, _)) => region,
                _ => return None,
            };

            if let Some(ref range) = self.range {
                if region.end() <= range.start {
                    continue;
                }

                if range.end <= region.start() {
                    break;
                }
            }

            return Some(Ok(region));
        }

        None
    }
}
