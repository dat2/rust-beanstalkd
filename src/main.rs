extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate combine;

use std::io;
use std::str;
use tokio_core::io::{Codec, EasyBuf, Io, Framed};
use tokio_proto::pipeline::ServerProto;
use tokio_service::Service;
use futures::{future, Future, BoxFuture};
use tokio_proto::TcpServer;

use combine::{Parser, State, try, none_of};
use combine::byte::{bytes, byte, digit, crlf};
use combine::combinator::many;

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
  StatsJob(usize)
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
               "{}{}\r\n",
               if let Some(_) = opt_timeout {
                 "reserve-with-timeout "
               } else {
                 "reserve"
               },
               if let Some(timeout) = opt_timeout {
                 timeout.to_string()
               } else {
                 String::new()
               })
      }
      Delete(id) => write!(f, "delete {}\r\n", id),
      Release(id, priority, delay) => write!(f, "release {} {} {}\r\n", id, priority, delay),
      Bury(id, priority) => write!(f, "bury {} {}\r\n", id, priority),
      Touch(id) => write!(f, "touch {}\r\n", id),
      Watch(ref tube) => write!(f, "watch {}\r\n", tube),
      Ignore(ref tube) => write!(f, "ignore {}\r\n", tube),
      Peek(id) => write!(f, "peek {}\r\n", id),
      PeekReady(id) => write!(f, "peek-ready\r\n", id),
      PeekDelayed(id) => write!(f, "peek-delayed\r\n", id),
      PeekBuried(id) => write!(f, "peek-buried\r\n", id),
      Kick(bound) => write!(f, "kick {}\r\n", bound)
      KickJob(id) => write!(f, "kick-job {}\r\n", id)
      StatsJob(id) => write!(f, "stats-job {}\r\n", id)
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
  Ok(Vec<u8>)
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
        write!(f, "KICKED{}\r\n", if let Some(b) = bound { format!("{} ", b) } else { String::new() })
      },
      Ok(data) => write!(f, "OK {}\r\n{}\r\n", data.len(), "")
    }
  }
}

// Codec
pub struct LineCodec;

impl Codec for LineCodec {
  type In = BeanstalkCommand;
  type Out = String;

  fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<Self::In>> {
    // put <pri> <delay> <ttr> <bytes>\r\n<data>\r\n
    let put = bytes(b"put")
      .skip(byte(b' '))
      .with(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(byte(b' '))
      .and(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(byte(b' '))
      .and(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(byte(b' '))
      .and(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(crlf())
      .and(many(none_of(b"\r\n".iter().cloned())))
      .skip(crlf())
      .map(|((((priority, delay), ttr), n_bytes), bytes)| {
        BeanstalkCommand::Put(usize::from_str_radix(&priority, 10).unwrap(),
                              usize::from_str_radix(&delay, 10).unwrap(),
                              std::cmp::min(1, usize::from_str_radix(&ttr, 10).unwrap()),
                              usize::from_str_radix(&n_bytes, 10).unwrap(),
                              bytes)
      });

    // use <tube>\r\n
    let use_parser = bytes(b"use")
      .skip(byte(b' '))
      .skip(crlf())
      .map(|_| BeanstalkCommand::Use(String::new()));

    // reserve\r\n
    let reserve = bytes(b"reserve")
      .skip(crlf())
      .map(|_: &'static [u8]| -> BeanstalkCommand { BeanstalkCommand::Reserve(None) });

    // reserve-with-timeout <seconds>\r\n
    let reserve_with_timeout = bytes(b"reserve-with-timeout")
      .skip(byte(b' '))
      .with(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(crlf())
      .map(|seconds| BeanstalkCommand::Reserve(Some(usize::from_str_radix(&seconds, 10).unwrap())));

    // delete <id>\r\n
    let delete = bytes(b"delete")
      .skip(byte(b' '))
      .with(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(crlf())
      .map(|id| BeanstalkCommand::Delete(usize::from_str_radix(&id, 10).unwrap()));

    // release <id> <pri> <delay>\r\n
    let release = bytes(b"release")
      .skip(byte(b' '))
      .with(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(byte(b' '))
      .and(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(byte(b' '))
      .and(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(crlf())
      .map(|((id, priority), delay)| {
        BeanstalkCommand::Release(usize::from_str_radix(&id, 10).unwrap(),
                                  usize::from_str_radix(&priority, 10).unwrap(),
                                  usize::from_str_radix(&delay, 10).unwrap())
      });

    // bury <id> <pri>\r\n
    let bury = bytes(b"bury")
      .skip(byte(b' '))
      .with(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(byte(b' '))
      .and(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(crlf())
      .map(|id| {
        BeanstalkCommand::Bury(usize::from_str_radix(&id, 10).unwrap(),
                               usize::from_str_radix(&bury, 10).unwrap())
      });

    // touch <id>\r\n
    let touch = bytes(b"touch")
      .skip(byte(b' '))
      .with(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(crlf())
      .map(|id| BeanstalkCommand::Touch(usize::from_str_radix(&id, 10).unwrap()));

    // watch <tube>\r\n
    let watch = bytes(b"watch")
      .skip(byte(b' '))
      .skip(crlf())
      .map(|_| BeanstalkCommand::Watch(String::new()));

    // ignore <tube>\r\n
    let ignore = bytes(b"ignore")
      .skip(byte(b' '))
      .skip(crlf())
      .map(|_| BeanstalkCommand::Ignore(String::new()));

    // peek <id>\r\n
    let ignore = bytes(b"peek")
      .skip(byte(b' '))
      .with(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(crlf())
      .map(|id| BeanstalkCommand::Peek(usize::from_str_radix(&id, 10).unwrap()));

    // peek-ready\r\n
    let peek_ready = bytes(b"peek-ready")
      .skip(crlf())
      .map(|_| BeanstalkCommand::PeekReady);

    // peek-delayed\r\n
    let peek_delayed = bytes(b"peek-delayed")
      .skip(crlf())
      .map(|_| BeanstalkCommand::PeekDelayed);

    // kick <bound>\r\n
    let ignore = bytes(b"kick")
      .skip(byte(b' '))
      .with(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(crlf())
      .map(|bound| BeanstalkCommand::Kick(usize::from_str_radix(&bound, 10).unwrap()));

    // kick-job <id>\r\n
    let kick_job = bytes(b"kick-job")
      .skip(byte(b' '))
      .with(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(crlf())
      .map(|id| BeanstalkCommand::KickJob(usize::from_str_radix(&id, 10).unwrap()));

    // stats-job <id>\r\n
    let kick_job = bytes(b"stats-job")
      .skip(byte(b' '))
      .with(many::<Vec<_>, _>(digit()).map(|ds| String::from_utf8(ds).unwrap()))
      .skip(crlf())
      .map(|id| BeanstalkCommand::StatsJob(usize::from_str_radix(&id, 10).unwrap()));

    let mut parser = put.or(use_parser)
      .or(try(reserve))
      .or(reserve_with_timeout)
      .or(delete)
      .or(release)
      .or(bury)
      .or(touch)
      .or(watch)
      .or(ignore)
      .or(peek)
      .or(peek_ready)
      .or(peek_delayed)
      .or(kick)
      .or(kick_job)
      .or(stats_job);

    match parser.parse(State::new(buf.clone().as_slice())) {
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
