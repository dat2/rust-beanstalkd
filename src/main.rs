#![recursion_limit = "1024"]
extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate combine;

use std::io;
use std::str;
use std::marker::PhantomData;
use tokio_core::io::{Codec, EasyBuf, Io, Framed};
use tokio_proto::pipeline::ServerProto;
use tokio_service::Service;
use futures::{future, Future, BoxFuture};
use tokio_proto::TcpServer;

use combine::primitives::{Parser, Stream, State, ParseResult};
use combine::byte::{bytes, byte, digit, crlf, alpha_num};
use combine::combinator::{FnParser, parser, many, count, none_of, try};

// https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt
// https://tokio.rs/docs/getting-started/simple-server/
// https://github.com/silver-lang/silver-rust/blob/master/src/parser.rs

// beanstalkd error messages
pub enum BeanstalkError {
  OutOfMemory,
  InternalError,
  BadFormat,
  UnknownCommand,
  ExpectedCrlf,
  JobTooBig,
  Draining,
}

impl std::fmt::Display for BeanstalkError {
  fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    use BeanstalkError::*;

    match *self {
      OutOfMemory => write!(f, "OUT_OF_MEMORY\r\n"),
      InternalError => write!(f, "INTERNAL_ERROR\r\n"),
      BadFormat => write!(f, "BAD_FORMAT\r\n"),
      UnknownCommand => write!(f, "UNKNOWN_COMMAND\r\n"),
      ExpectedCrlf => write!(f, "EXPECTED_CRLF\r\n"),
      JobTooBig => write!(f, "JOB_TOO_BIG\r\n"),
      Draining => write!(f, "DRAINING\r\n"),
    }
  }
}

#[derive(Debug)]
pub enum BeanstalkCommand {
  Put(usize, usize, usize, usize, Vec<u8>),
  Use(String),
  Reserve(Option<usize>),
  Delete(usize),
  Release(usize, usize, usize),
  Bury(usize, usize),
  Touch(usize),
  Watch(String),
  Ignore(String),
  Peek(usize),
  PeekReady,
  PeekDelayed,
  PeekBuried,
  Kick(usize),
  KickJob(usize),
  StatsJob(usize),
  StatsTube(usize),
  Stats,
  ListTubes
}

impl std::fmt::Display for BeanstalkCommand {
  fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    use BeanstalkCommand::*;

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
      PeekBuried => write!(f, "peek-buried\r\n"),
      Kick(bound) => write!(f, "kick {}\r\n", bound),
      KickJob(id) => write!(f, "kick-job {}\r\n", id),
      StatsJob(id) => write!(f, "stats-job {}\r\n", id),
      StatsTube(id) => write!(f, "stats-tube {}\r\n", id),
      Stats => write!(f, "stats\r\n"),
      ListTubes => write!(f, "list-tubes\r\n"),
    }
  }
}

pub enum BeanstalkReply {
  Inserted(usize),
  Buried(Option<usize>),
  Using(String),
  Reserved(usize, Vec<u8>),
  DeadlineSoon,
  TimedOut,
  Deleted,
  NotFound,
  Released,
  Touched,
  Watching(usize),
  NotIgnored,
  Found(usize, Vec<u8>),
  Kicked(Option<usize>),
  Ok(Vec<u8>),
}

impl std::fmt::Display for BeanstalkReply {
  fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    use BeanstalkReply::*;

    match *self {
      Inserted(id) => write!(f, "INSERTED {}\r\n", id),
      Buried(id) => {
        write!(f,
               "BURIED{}\r\n",
               if let Some(i) = id {
                 format!(" {}", i)
               } else {
                 String::new()
               })
      }
      Using(ref tube) => write!(f, "USING {}\r\n", tube),
      Reserved(id, ref bytes) => write!(f, "RESERVED {} {}\r\n{}\r\n", id, bytes.len(), ""),
      DeadlineSoon => write!(f, "DEADLINE_SOON\r\n"),
      TimedOut => write!(f, "TIMED_OUT\r\n"),
      Deleted => write!(f, "DELETED\r\n"),
      NotFound => write!(f, "NOT_FOUND\r\n"),
      Released => write!(f, "RELEASED\r\n"),
      Touched => write!(f, "TOUCHED\r\n"),
      Watching(count) => write!(f, "WATCHING {}\r\n", count),
      NotIgnored => write!(f, "NOT_IGNORED\r\n"),
      Found(id, ref bytes) => write!(f, "FOUND {} {}\r\n{}\r\n", id, bytes.len(), ""),
      Kicked(bound) => {
        write!(f,
               "KICKED{}\r\n",
               if let Some(b) = bound {
                 format!("{} ", b)
               } else {
                 String::new()
               })
      }
      Ok(ref data) => write!(f, "OK {}\r\n{}\r\n", data.len(), ""),
    }
  }
}

#[derive(Default)]
pub struct Beanstalk<I>(PhantomData<fn(I) -> I>);

type BeanstalkParser<O, I> = FnParser<I, fn(I) -> ParseResult<O, I>>;

fn fn_parser<O, I>(f: fn(I) -> ParseResult<O, I>) -> BeanstalkParser<O, I>
  where I: Stream<Item=u8>
{
  parser(f)
}

impl<'a, I> Beanstalk<I>
  where I: Stream<Item=u8, Range=&'a [u8]>
{
  fn name() -> BeanstalkParser<String, I> {
    fn_parser(Beanstalk::<I>::name_)
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

  fn number() -> BeanstalkParser<usize, I> {
    fn_parser(Beanstalk::<I>::number_)
  }

  fn number_(input: I) -> ParseResult<usize, I> {
    let mut parser = many::<Vec<_>, _>(digit())
      .map(|ds| String::from_utf8(ds).unwrap())
      .map(|digits| usize::from_str_radix(&digits, 10).unwrap());

    parser.parse_stream(input)
  }

  fn put() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::put_)
  }

  fn put_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // put <pri> <delay> <ttr> <bytes>\r\n<data>\r\n
    let mut put = bytes(b"put")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(byte(b' '))
      .and(Beanstalk::number())
      .skip(byte(b' '))
      .and(Beanstalk::number())
      .skip(byte(b' '))
      .and(Beanstalk::number())
      .skip(crlf())
      .and(many(none_of(b"\r\n".iter().cloned())))
      .skip(crlf())
      .map(|((((priority, delay), ttr), n_bytes), bytes)| {
        BeanstalkCommand::Put(priority, delay, ttr, n_bytes, bytes)
      });

    put.parse_stream(input)
  }

  fn use_parser() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::use_)
  }

  fn use_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // use <tube>\r\n
    let mut use_parser = bytes(b"use")
      .skip(byte(b' '))
      .with(Beanstalk::name())
      .skip(crlf())
      .map(BeanstalkCommand::Use);

    use_parser.parse_stream(input)
  }

  fn reserve() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::reserve_)
  }

  fn reserve_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // reserve\r\n
    let mut reserve = bytes(b"reserve")
      .skip(crlf())
      .map(|_: &'static [u8]| -> BeanstalkCommand { BeanstalkCommand::Reserve(None) });

    reserve.parse_stream(input)
  }

  fn reserve_with_timeout() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::reserve_with_timeout_)
  }

  fn reserve_with_timeout_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // reserve-with-timeout <seconds>\r\n
    let mut reserve_with_timeout = bytes(b"reserve-with-timeout")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(crlf())
      .map(|seconds| BeanstalkCommand::Reserve(Some(seconds)));

    reserve_with_timeout.parse_stream(input)
  }

  fn delete() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::delete_)
  }

  fn delete_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // delete <id>\r\n
    let mut delete = bytes(b"delete")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(crlf())
      .map(BeanstalkCommand::Delete);

    delete.parse_stream(input)
  }

  fn release() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::release_)
  }

  fn release_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // release <id> <pri> <delay>\r\n
    let mut release = bytes(b"release")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(byte(b' '))
      .and(Beanstalk::number())
      .skip(byte(b' '))
      .and(Beanstalk::number())
      .skip(crlf())
      .map(|((id, priority), delay)| BeanstalkCommand::Release(id, priority, delay));

    release.parse_stream(input)
  }

  fn bury() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::bury_)
  }

  fn bury_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // bury <id> <pri>\r\n
    let mut bury = bytes(b"bury")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(byte(b' '))
      .and(Beanstalk::number())
      .skip(crlf())
      .map(|(id, priority)| BeanstalkCommand::Bury(id, priority));

    bury.parse_stream(input)
  }

  fn touch() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::touch_)
  }

  fn touch_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // touch <id>\r\n
    let mut touch = bytes(b"touch")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(crlf())
      .map(BeanstalkCommand::Touch);

    touch.parse_stream(input)
  }

  fn watch() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::watch_)
  }

  fn watch_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // watch <tube>\r\n
    let mut watch = bytes(b"watch")
      .skip(byte(b' '))
      .with(Beanstalk::name())
      .skip(crlf())
      .map(BeanstalkCommand::Watch);

    watch.parse_stream(input)
  }

  fn ignore() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::ignore_)
  }

  fn ignore_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // ignore <tube>\r\n
    let mut ignore = bytes(b"ignore")
      .skip(byte(b' '))
      .with(Beanstalk::name())
      .skip(crlf())
      .map(BeanstalkCommand::Ignore);

    ignore.parse_stream(input)
  }

  fn peek() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::peek_)
  }

  fn peek_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // peek <id>\r\n
    let mut peek = bytes(b"peek")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(crlf())
      .map(BeanstalkCommand::Peek);

    peek.parse_stream(input)
  }

  fn peek_ready() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::peek_ready_)
  }

  fn peek_ready_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // peek-ready\r\n
    let mut peek_ready = bytes(b"peek-ready")
      .skip(crlf())
      .map(|_| BeanstalkCommand::PeekReady);

    peek_ready.parse_stream(input)
  }

  fn peek_delayed() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::peek_delayed_)
  }

  fn peek_delayed_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // peek-delayed\r\n
    let mut peek_delayed = bytes(b"peek-delayed")
      .skip(crlf())
      .map(|_| BeanstalkCommand::PeekDelayed);

    peek_delayed.parse_stream(input)
  }

  fn kick() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::kick_)
  }

  fn kick_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // kick <bound>\r\n
    let mut kick = bytes(b"kick")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(crlf())
      .map(BeanstalkCommand::Kick);

    kick.parse_stream(input)
  }

  fn kick_job() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::kick_job_)
  }

  fn kick_job_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // kick-job <id>\r\n
    let mut kick_job = bytes(b"kick-job")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(crlf())
      .map(BeanstalkCommand::KickJob);

    kick_job.parse_stream(input)
  }

  fn stats_job() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::stats_job_)
  }

  fn stats_job_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // stats-job <id>\r\n
    let mut stats_job = bytes(b"stats-job")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(crlf())
      .map(BeanstalkCommand::StatsJob);

    stats_job.parse_stream(input)
  }

  fn stats_tube() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::stats_tube_)
  }

  fn stats_tube_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // stats-job <id>\r\n
    let mut stats_tube = bytes(b"stats-tube")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(crlf())
      .map(BeanstalkCommand::StatsTube);

    stats_tube.parse_stream(input)
  }

  fn stats() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::stats_)
  }

  fn stats_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // stats-job <id>\r\n
    let mut stats = bytes(b"stats")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(crlf())
      .map(|_| BeanstalkCommand::Stats);

    stats.parse_stream(input)
  }

  fn list_tubes() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::list_tubes_)
  }

  fn list_tubes_(input: I) -> ParseResult<BeanstalkCommand, I> {
    // list_tubes-job <id>\r\n
    let mut list_tubes = bytes(b"list-tubes")
      .skip(byte(b' '))
      .with(Beanstalk::number())
      .skip(crlf())
      .map(|_| BeanstalkCommand::ListTubes);

    list_tubes.parse_stream(input)
  }

  fn command() -> BeanstalkParser<BeanstalkCommand, I> {
    fn_parser(Beanstalk::<I>::command_)
  }

  fn command_(input: I) -> ParseResult<BeanstalkCommand, I> {
    let mut parser = Beanstalk::put()
      .or(Beanstalk::use_parser())
      .or(try(Beanstalk::reserve()))
      .or(Beanstalk::reserve_with_timeout())
      .or(Beanstalk::delete())
      .or(Beanstalk::release())
      .or(Beanstalk::bury())
      .or(Beanstalk::touch())
      .or(Beanstalk::watch())
      .or(Beanstalk::ignore())
      .or(Beanstalk::peek())
      .or(Beanstalk::peek_ready())
      .or(Beanstalk::peek_delayed())
      .or(Beanstalk::kick())
      .or(Beanstalk::kick_job())
      .or(Beanstalk::stats_job())
      .or(Beanstalk::stats_tube())
      .or(Beanstalk::stats())
      .or(Beanstalk::list_tubes());

    parser.parse_stream(input)
  }
}

// Codec
pub struct LineCodec;

impl Codec for LineCodec {
  type In = BeanstalkCommand;
  type Out = String;

  fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<Self::In>> {

    match Beanstalk::command().parse(State::new(buf.clone().as_slice())) {
      Ok((value, state)) => {
        buf.drain_to(state.position.position);
        Ok(Some(value))
      }
      Err(e) => {
        println!("{:?}", e);
        Ok(None)
      }
    }
  }

  fn encode(&mut self, msg: String, buf: &mut Vec<u8>) -> io::Result<()> {
    buf.extend(msg.as_bytes());
    buf.push(b'\n');
    Ok(())
  }
}

// Protocol
pub struct LineProto;

impl<T: Io + 'static> ServerProto<T> for LineProto {
  // request matches codec in type
  type Request = BeanstalkCommand;

  // response matches codec out type
  type Response = String;

  // hook in the codec
  type Transport = Framed<T, LineCodec>;
  type BindTransport = Result<Self::Transport, io::Error>;

  fn bind_transport(&self, io: T) -> Self::BindTransport {
    Ok(io.framed(LineCodec))
  }
}

// Service
pub struct Echo;

impl Service for Echo {
  // must match protocol
  type Request = BeanstalkCommand;
  type Response = String;

  // non streaming protocols, service errors are always io::Error
  type Error = io::Error;

  // future for computing the response
  type Future = BoxFuture<Self::Response, Self::Error>;

  fn call(&self, req: Self::Request) -> Self::Future {
    future::ok(format!("{}", req)).boxed()
  }
}

fn main() {
  let addr = "127.0.0.1:11300".parse().unwrap();

  let server = TcpServer::new(LineProto, addr);

  server.serve(|| Ok(Echo));
}
