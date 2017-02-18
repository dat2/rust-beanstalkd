use std::marker::PhantomData;
use std::fmt;

use combine::primitives::{Parser, Stream, ParseResult};
use combine::byte::{bytes, byte, digit, crlf, alpha_num};
use combine::combinator::{FnParser, parser, many, count, none_of, try};

#[derive(Debug)]
pub enum BeanstalkCommand {
  Put(u32, u32, u32, u32, Vec<u8>),
  Use(String),
  Reserve(Option<u32>),
  Delete(u32),
  Release(u32, u32, u32),
  Bury(u32, u32),
  Touch(u32),
  Watch(String),
  Ignore(String),
  Peek(u32),
  PeekReady,
  PeekDelayed,
  PeekBuried,
  Kick(u32),
  KickJob(u32),
  StatsJob(u32),
  StatsTube(String),
  Stats,
  ListTubes,
  ListTubeUsed,
  ListTubesWatched,
  Quit,
  PauseTube(String, u32),
  Unknown,
}

impl fmt::Display for BeanstalkCommand {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    use self::BeanstalkCommand::*;

    match *self {
      Put(priority, delay, ttr, bytes, ref data) => {
        write!(f,
               "put {} {} {} {}\r\n{:?}\r\n",
               priority,
               delay,
               ttr,
               bytes,
               data)
      }
      Use(ref tube) => write!(f, "use {}\r\n", tube),
      Reserve(opt_timeout) => {
        write!(f,
               "{}\r\n",
               if opt_timeout.is_some() {
                 format!("reserve-with-timeout {}", opt_timeout.unwrap())
               } else {
                 String::from("reserve")
               })
      }
      Delete(id) => write!(f, "delete {}\r\n", id),
      Release(id, priority, delay) => write!(f, "release {} {} {}\r\n", id, priority, delay),
      Bury(id, priority) => write!(f, "bury {} {}\r\n", id, priority),
      Touch(id) => write!(f, "touch {}\r\n", id),
      Watch(ref tube) => write!(f, "watch {}\r\n", tube),
      Ignore(ref tube) => write!(f, "ignore {}\r\n", tube),
      Peek(id) => write!(f, "peek {}\r\n", id),
      PeekReady => write!(f, "peek-ready\r\n"),
      PeekDelayed => write!(f, "peek-delayed\r\n"),
      PeekBuried => write!(f, "peek-buried\r\n"), // doesn't seem to work
      Kick(bound) => write!(f, "kick {}\r\n", bound),
      KickJob(id) => write!(f, "kick-job {}\r\n", id),
      StatsJob(id) => write!(f, "stats-job {}\r\n", id),
      StatsTube(ref tube) => write!(f, "stats-tube {}\r\n", tube),
      Stats => write!(f, "stats\r\n"),
      ListTubes => write!(f, "list-tubes\r\n"),
      ListTubeUsed => write!(f, "list-tube-used\r\n"),
      ListTubesWatched => write!(f, "list-tubes-watched\r\n"),
      Quit => write!(f, "quit\r\n"),
      PauseTube(ref tube, delay) => write!(f, "pause-tube {} {}\r\n", tube, delay),
      Unknown => write!(f, "<unknown>\r\n"),
    }
  }
}

#[derive(Default)]
pub struct BeanstalkCommandParser<I>(PhantomData<fn(I) -> I>);

type BeanstalkParser<O, I> = FnParser<I, fn(I) -> ParseResult<O, I>>;

fn fn_parser<O, I>(f: fn(I) -> ParseResult<O, I>) -> BeanstalkParser<O, I>
  where I: Stream<Item = u8>
{
  parser(f)
}

impl<'a, I> BeanstalkCommandParser<I>
  where I: Stream<Item = u8, Range = &'a [u8]>
{
  fn name() -> BeanstalkParser<String, I> {
    fn_parser(BeanstalkCommandParser::name_)
  }

  fn name_(input: I) -> ParseResult<String, I> {
    let name_char = alpha_num()
      .or(byte(b'-'))
      .or(byte(b'+'))
      .or(byte(b'/'))
      .or(byte(b';'))
      .or(byte(b'.'))
      .or(byte(b'$'))
      .or(byte(b'_'))
      .or(byte(b'('))
      .or(byte(b')'));

    count::<Vec<u8>, _>(200, name_char)
      .map(|name| String::from_utf8(name).unwrap())
      .parse_stream(input)
  }

  fn number() -> BeanstalkParser<u32, I> {
    fn_parser(BeanstalkCommandParser::number_)
  }

  fn number_(input: I) -> ParseResult<u32, I> {
    let mut parser = many::<Vec<_>, _>(digit())
      .map(|ds| String::from_utf8(ds).unwrap())
      .map(|digits| u32::from_str_radix(&digits, 10).unwrap());

    parser.parse_stream(input)
  }

  fn put() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::put_)
  }

  fn put_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // put <pri> <delay> <ttr> <bytes>\r\n<data>\r\n
    let mut put = bytes(b"put")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::number())
      .skip(byte(b' '))
      .and(BeanstalkCommandParser::number())
      .skip(byte(b' '))
      .and(BeanstalkCommandParser::number())
      .skip(byte(b' '))
      .and(BeanstalkCommandParser::number())
      .skip(crlf())
      .and(many(none_of(b"\r\n".iter().cloned())))
      .skip(crlf())
      .map(|((((priority, delay), ttr), n_bytes), bytes)| {
        BeanstalkCommand::Put(priority, delay, ttr, n_bytes, bytes)
      });

    put.parse_stream(input)
  }

  fn use_parser() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::use_)
  }

  fn use_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // use <tube>\r\n
    let mut use_parser = bytes(b"use")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::name())
      .skip(crlf())
      .map(BeanstalkCommand::Use);

    use_parser.parse_stream(input)
  }

  fn reserve() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::reserve_)
  }

  fn reserve_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // reserve\r\n
    let mut reserve = bytes(b"reserve")
      .skip(crlf())
      .map(|_: &'static [u8]| -> BeanstalkCommand { BeanstalkCommand::Reserve(None) });

    reserve.parse_stream(input)
  }

  fn reserve_with_timeout() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::reserve_with_timeout_)
  }

  fn reserve_with_timeout_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // reserve-with-timeout <seconds>\r\n
    let mut reserve_with_timeout = bytes(b"reserve-with-timeout")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::number())
      .skip(crlf())
      .map(|seconds| BeanstalkCommand::Reserve(Some(seconds)));

    reserve_with_timeout.parse_stream(input)
  }

  fn delete() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::delete_)
  }

  fn delete_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // delete <id>\r\n
    let mut delete = bytes(b"delete")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::number())
      .skip(crlf())
      .map(BeanstalkCommand::Delete);

    delete.parse_stream(input)
  }

  fn release() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::release_)
  }

  fn release_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // release <id> <pri> <delay>\r\n
    let mut release = bytes(b"release")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::number())
      .skip(byte(b' '))
      .and(BeanstalkCommandParser::number())
      .skip(byte(b' '))
      .and(BeanstalkCommandParser::number())
      .skip(crlf())
      .map(|((id, priority), delay)| BeanstalkCommand::Release(id, priority, delay));

    release.parse_stream(input)
  }

  fn bury() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::bury_)
  }

  fn bury_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // bury <id> <pri>\r\n
    let mut bury = bytes(b"bury")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::number())
      .skip(byte(b' '))
      .and(BeanstalkCommandParser::number())
      .skip(crlf())
      .map(|(id, priority)| BeanstalkCommand::Bury(id, priority));

    bury.parse_stream(input)
  }

  fn touch() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::touch_)
  }

  fn touch_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // touch <id>\r\n
    let mut touch = bytes(b"touch")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::number())
      .skip(crlf())
      .map(BeanstalkCommand::Touch);

    touch.parse_stream(input)
  }

  fn watch() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::watch_)
  }

  fn watch_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // watch <tube>\r\n
    let mut watch = bytes(b"watch")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::name())
      .skip(crlf())
      .map(BeanstalkCommand::Watch);

    watch.parse_stream(input)
  }

  fn ignore() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::ignore_)
  }

  fn ignore_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // ignore <tube>\r\n
    let mut ignore = bytes(b"ignore")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::name())
      .skip(crlf())
      .map(BeanstalkCommand::Ignore);

    ignore.parse_stream(input)
  }

  fn peek() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::peek_)
  }

  fn peek_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // peek <id>\r\n
    let mut peek = bytes(b"peek")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::number())
      .skip(crlf())
      .map(BeanstalkCommand::Peek);

    peek.parse_stream(input)
  }

  fn peek_ready() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::peek_ready_)
  }

  fn peek_ready_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // peek-ready\r\n
    let mut peek_ready = bytes(b"peek-ready")
      .skip(crlf())
      .map(|_| BeanstalkCommand::PeekReady);

    peek_ready.parse_stream(input)
  }

  fn peek_delayed() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::peek_delayed_)
  }

  fn peek_delayed_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // peek-delayed\r\n
    let mut peek_delayed = bytes(b"peek-delayed")
      .skip(crlf())
      .map(|_| BeanstalkCommand::PeekDelayed);

    peek_delayed.parse_stream(input)
  }

  fn peek_buried() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::peek_buried_)
  }

  fn peek_buried_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // peek-buried\r\n
    let mut peek_buried = bytes(b"peek-buried")
      .skip(crlf())
      .map(|_| BeanstalkCommand::PeekBuried);

    peek_buried.parse_stream(input)
  }

  fn kick() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::kick_)
  }

  fn kick_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // kick <bound>\r\n
    let mut kick = bytes(b"kick")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::number())
      .skip(crlf())
      .map(BeanstalkCommand::Kick);

    kick.parse_stream(input)
  }

  fn kick_job() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::kick_job_)
  }

  fn kick_job_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // kick-job <id>\r\n
    let mut kick_job = bytes(b"kick-job")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::number())
      .skip(crlf())
      .map(BeanstalkCommand::KickJob);

    kick_job.parse_stream(input)
  }

  fn stats_job() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::stats_job_)
  }

  fn stats_job_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // stats-job <id>\r\n
    let mut stats_job = bytes(b"stats-job")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::number())
      .skip(crlf())
      .map(BeanstalkCommand::StatsJob);

    stats_job.parse_stream(input)
  }

  fn stats_tube() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::stats_tube_)
  }

  fn stats_tube_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // stats-tube <tube>\r\n
    let mut stats_tube = bytes(b"stats-tube")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::name())
      .skip(crlf())
      .map(BeanstalkCommand::StatsTube);

    stats_tube.parse_stream(input)
  }

  fn stats() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::stats_)
  }

  fn stats_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // stats\r\n
    let mut stats = bytes(b"stats")
      .skip(crlf())
      .map(|_| BeanstalkCommand::Stats);

    stats.parse_stream(input)
  }

  fn list_tubes() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::list_tubes_)
  }

  fn list_tubes_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // list-tubes <id>\r\n
    let mut list_tubes = bytes(b"list-tubes")
      .skip(crlf())
      .map(|_| BeanstalkCommand::ListTubes);

    list_tubes.parse_stream(input)
  }

  fn list_tube_used() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::list_tube_used_)
  }

  fn list_tube_used_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // list-tube-used\r\n
    let mut list_tube_used = bytes(b"list-tube-used")
      .skip(crlf())
      .map(|_| BeanstalkCommand::ListTubeUsed);

    list_tube_used.parse_stream(input)
  }

  fn list_tubes_watched() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::list_tubes_watched_)
  }

  fn list_tubes_watched_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // list-tubes-watched\r\n
    let mut list_tubes_watched = bytes(b"list-tubes-watched")
      .skip(crlf())
      .map(|_| BeanstalkCommand::ListTubesWatched);

    list_tubes_watched.parse_stream(input)
  }

  fn quit() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::quit_)
  }

  fn quit_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // quit\r\n
    let mut quit = bytes(b"quit")
      .skip(crlf())
      .map(|_| BeanstalkCommand::Quit);

    quit.parse_stream(input)
  }

  fn pause_tube() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::pause_tube_)
  }

  fn pause_tube_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // pause_tube <tube> <delay>\r\n
    let mut pause_tube = bytes(b"pause-tube")
      .skip(byte(b' '))
      .with(BeanstalkCommandParser::name())
      .skip(byte(b' '))
      .and(BeanstalkCommandParser::number())
      .skip(crlf())
      .map(|(tube, delay)| BeanstalkCommand::PauseTube(tube, delay));

    pause_tube.parse_stream(input)
  }

  pub fn command() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(BeanstalkCommandParser::command_)
  }

  fn command_(input: I) -> ParseResult<BeanstalkCommand, I> {
    let mut parser = try(BeanstalkCommandParser::put())
      .or(try(BeanstalkCommandParser::use_parser()))
      .or(try(BeanstalkCommandParser::reserve()))
      .or(try(BeanstalkCommandParser::reserve_with_timeout()))
      .or(try(BeanstalkCommandParser::delete()))
      .or(try(BeanstalkCommandParser::release()))
      .or(try(BeanstalkCommandParser::bury()))
      .or(try(BeanstalkCommandParser::touch()))
      .or(try(BeanstalkCommandParser::watch()))
      .or(try(BeanstalkCommandParser::ignore()))
      .or(try(BeanstalkCommandParser::peek()))
      .or(try(BeanstalkCommandParser::peek_ready()))
      .or(try(BeanstalkCommandParser::peek_buried()))
      .or(try(BeanstalkCommandParser::peek_delayed()))
      .or(try(BeanstalkCommandParser::kick()))
      .or(try(BeanstalkCommandParser::kick_job()))
      .or(try(BeanstalkCommandParser::stats()))
      .or(try(BeanstalkCommandParser::stats_job()))
      .or(try(BeanstalkCommandParser::stats_tube()))
      .or(try(BeanstalkCommandParser::list_tubes()))
      .or(try(BeanstalkCommandParser::list_tube_used()))
      .or(try(BeanstalkCommandParser::list_tubes_watched()))
      .or(try(BeanstalkCommandParser::quit()))
      .or(BeanstalkCommandParser::pause_tube());

    parser.parse_stream(input)
  }
}
